(*******************************************************************************
	This file is a part of x264farm.

	x264farm is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation; either version 2 of the License, or
	(at your option) any later version.

	x264farm is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with x264farm; if not, write to the Free Software
	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*******************************************************************************)

open Types;;
open Agentinclude;;
print "x264farm agent version %s\n" version;;

open Pack;;
open Net;;
open Matroska;;

let debug_file = true;;

let config_ref = ref "config.xml";;
let buffersize_ref = ref 1382401;; (* Will hold 1 1280x720 frame *)
let stale_ref = ref 604800.0;; (* 1 week *)
let logfile_ref = ref "out-dump.txt";;
let ad_hoc_ref = ref true;; (* Whether or not to listen for controllers and ping when set up *)
(*let ad_hoc_name_ref = ref "";;*)

let arg_parse = Arg.align [
(*	("--port", Arg.Set_int port_ref, " Listening port (15086)");*)
	("--config",      Arg.Set_string config_ref,      (Printf.sprintf " Location of the config XML file (\"%s\")" !config_ref));
	("--buffersize",  Arg.Set_int buffersize_ref,     (Printf.sprintf " Network buffer size for controller encoding (%d)" !buffersize_ref));
	("--stale",       Arg.Set_float stale_ref,        (Printf.sprintf " Max seconds before a temp file is deleted (%.0f)" !stale_ref));
	("--logfile",     Arg.Set_string logfile_ref,     (Printf.sprintf " Location of the log file (%S)" !logfile_ref));
	("--noadhoc",     Arg.Clear ad_hoc_ref,           (Printf.sprintf " Turns off response to ad-hoc agent discovery"));
(*	("--name",        Arg.Set_string ad_hoc_name_ref, (Printf.sprintf " Overrides the name given to the controller in ad-hoc mode"));*)
];;

(* Set the config file to whatever the last argument was *)
Arg.parse arg_parse (fun x -> config_ref := x) (Printf.sprintf "Distributed encoding AGENT (version %s)" version);;

(* Parse the logfile location first, to make sure as much is printed to it *)
(try
	Agentinclude.print_handle_ref := Some (open_out !logfile_ref);
	print ~screen:false "x264farm agent version %s\n" version;
with
	Sys_error _ -> print "WARNING: Log file \"%s\" can not be written!\n" !logfile_ref
);;


let buffersize = if !buffersize_ref < 8193 then (
	print "Buffersize must be 8193 or more; setting to 8193\n";
	8193
) else if !buffersize_ref > Sys.max_string_length then (
	print "Buffersize must be less than %d; setting to %d\n" Sys.max_string_length Sys.max_string_length;
	Sys.max_string_length
) else (
	!buffersize_ref
);;

let stale = if !stale_ref <= 0.0 then (
	print "Stale seconds very small; turning off stale file deletion\n";
	None
) else if !stale_ref < 60.0 then (
	print "Stale seconds must be 60 or more; setting to 60\n";
	Some 60.0
) else if !stale_ref > 5e17 then (
	print "Stale seconds longer than the age of the universe; turning off\n";
	None
) else (
	Some !stale_ref
);;


(*************************)
(* Parse the config file *)
(*************************)
let parse_config_file file = (
	let file = match (search_for_file file, search_for_file "config.xml") with
		| (Some x, _) -> x
		| (None, Some y) -> (print "WARNING: Config file \"%s\" does not exist; using \"%s\" instead\n" file y; y)
		| (None, None) -> (print "ERROR: No configuration file found; exiting\n"; exit 2)
	in
	print "Using config file \"%s\"\n" file;

	let temp_dir_ref = ref "" in
	let x264_exe_ref = ref "x264" in
	let port_from_ref = ref 40700 in
	let port_to_ref   = ref 40703 in
	let nice_ref = ref 0 in
	let number_ref = ref 1 in
	let number_pad_ref = ref 0 in
	let base_list_ref = ref [] in
	let pipe_ref = ref false in
	let compression1_string_ref = ref "\x00" in
	let compression2_string_ref = ref "\x00" in
	let ad_hoc_controller_ref = ref None in
	let ad_hoc_agent_ref = ref None in
	let ad_hoc_name_ref = ref "" in

	let parse_config = (function
		| Xml.Element ("temp", _, [Xml.PCData x]) -> temp_dir_ref := x
		| Xml.Element ("port", ([("from",x); ("to",y)] | [("to",y); ("from",x)]), []) -> (port_from_ref := int_of_string x; port_to_ref := int_of_string y)
		| Xml.Element ("port", _, [Xml.PCData x]) -> (port_from_ref := int_of_string x; port_to_ref := int_of_string x)
		| Xml.Element ("x264", _, [Xml.PCData x]) -> x264_exe_ref := x
		| Xml.Element ("nice", _, [Xml.PCData x]) -> nice_ref := int_of_string x
		| Xml.Element ("number",[("pad",y)],[Xml.PCData x]) -> (number_ref := int_of_string x; number_pad_ref := int_of_string y)
		| Xml.Element ("number",_,[Xml.PCData x]) -> number_ref := int_of_string x
		| Xml.Element ("base", _, []) -> (base_list_ref := [""]) (* This lets one use <base/> as meaning whatever the file is at the controller side *)
		| Xml.Element ("base", _, [Xml.PCData x]) -> (
			if is_dir x then (
				base_list_ref := [x]
			) else (
				print "WARNING: cannot find base directory \"%s\"; ignoring\n" x;
			)
		)
		| Xml.Element ("bases", _, base_list) -> (
			(* Multiple bases *)
			let rec iterate = function
				| [] -> []
				| (Xml.Element ("base", _, [])) :: tl -> "" :: iterate tl
				| (Xml.Element ("base", _, [Xml.PCData x])) :: tl when is_dir x -> x :: iterate tl
				| (Xml.Element ("base", _, [Xml.PCData x])) :: tl -> (print "WARNING: cannot find base directory \"%s\"; ignoring\n" x; iterate tl)
				| hd :: tl -> iterate tl
			in
			base_list_ref := iterate base_list
		)
		| Xml.Element ("agentpipe", _, [Xml.PCData x]) when x = "0" || x = "" -> pipe_ref := false
		| Xml.Element ("agentpipe", _, [Xml.PCData x]) -> pipe_ref := true
		| Xml.Element ("compression", _, type_list) -> (
			let rec iterate x = (
				match x with
				| [] -> []
				| (Xml.Element ("type", _, [Xml.PCData t])) :: tl -> (Char.chr (int_of_string t)) :: iterate tl
				| hd :: tl -> iterate tl
			) in
			let char_array = Array.of_list (iterate type_list) in
			let type_string = String.create (Array.length char_array) in
			Array.iteri (fun i x -> type_string.[i] <- x) char_array;
			compression1_string_ref := type_string;
			compression2_string_ref := type_string;
		)
		| Xml.Element ("passcompression", _, pass_list) -> (
			let rec iterate_type x = (
				match x with
				| [] -> []
				| (Xml.Element ("type", _, [Xml.PCData t])) :: tl -> (Char.chr (int_of_string t)) :: iterate_type tl
				| hd :: tl -> iterate_type tl
			) in
			let rec iterate_pass p = (
				match p with
				| [] -> ()
				| (Xml.Element ("first", _, type_list)) :: tl -> (
					let char_array = Array.of_list (iterate_type type_list) in
					let type_string = String.create (Array.length char_array) in
					Array.iteri (fun i x -> type_string.[i] <- x) char_array;
					compression1_string_ref := type_string;
					iterate_pass tl
				)
				| (Xml.Element ("second", _, type_list)) :: tl -> (
					let char_array = Array.of_list (iterate_type type_list) in
					let type_string = String.create (Array.length char_array) in
					Array.iteri (fun i x -> type_string.[i] <- x) char_array;
					compression2_string_ref := type_string;
					iterate_pass tl
				)
				| hd :: tl -> iterate_pass tl
			) in
			iterate_pass pass_list
		)
		| Xml.Element ("adhoc", ([("agent",a);("controller",c)]|[("controller",c);("agent",a)]), []) -> (
			ad_hoc_controller_ref := Some (int_of_string c);
			ad_hoc_agent_ref := Some (int_of_string a);
		)
		| Xml.Element ("adhoc", [("agent",a)], _) -> ad_hoc_agent_ref := Some (int_of_string a)
		| Xml.Element ("adhoc", [("controller",c)], _) -> ad_hoc_controller_ref := Some (int_of_string c)
		| Xml.Element ("name", _, [Xml.PCData x]) -> ad_hoc_name_ref := x
		| _ -> ()
	) in

	let parse_root = (function
		| Xml.Element ("config", _, x) -> List.iter parse_config x
		| _ -> (print "ERROR: root XML element of config file must be <config>\n"; exit 1)
	) in

	parse_root (Xml.parse_file file);

	if not (is_dir !temp_dir_ref) then (
		print "ERROR: temp dir \"%s\" given in file \"%s\" is not a directory\n" !temp_dir_ref file;
		exit 1
	);

	let ad_hoc_controller = (match !ad_hoc_controller_ref with | Some x -> x | None -> 0) in
	let ad_hoc_agent      = (match !ad_hoc_agent_ref      with | Some x -> x | None -> 0) in

	(!temp_dir_ref, (min !port_from_ref !port_to_ref), (max !port_from_ref !port_to_ref), !x264_exe_ref, !nice_ref, !number_ref, !number_pad_ref, !base_list_ref, !pipe_ref, !compression1_string_ref, !compression2_string_ref, ad_hoc_controller, ad_hoc_agent, !ad_hoc_name_ref)

);;

let (temp_dir, agent_port_from, agent_port_to, x264_exe, nice, number_of_agents, number_of_agents_pad, base_list, pipe, compression1_string, compression2_string, ad_hoc_controller, ad_hoc_agent, ad_hoc_name) = parse_config_file !config_ref;;
if debug_file then (
	print "  Temp dir: \"%s\"\n" temp_dir;
	(if agent_port_from = agent_port_to then print "  Port: %d\n" agent_port_from else print "  Ports: %d-%d\n" agent_port_from agent_port_to);
	print "  x264: \"%s\"\n" x264_exe;
	print "  nice: %d\n" nice;
	print "  agents: (%d,%d)\n" number_of_agents (number_of_agents + number_of_agents_pad);
	(match base_list with
		| [] -> print "  agent bases: [None]\n";
		| _ -> (
			print "  agent bases:\n";
			List.iter (function "" -> print "    [exact]\n" | x -> print "    \"%s\"\n" x) base_list
		)
	);
(*	print " agent bases: %s\n" (match base_list with | None -> "[None]" | Some x -> "\"" ^ x ^ "\"");*)
	print "  pipe: %B\n" pipe;
	print "  compressions: %s - %s\n" (to_hex compression1_string) (to_hex compression2_string);
	print "  deleting old files: %s\n" (match stale with None -> "no" | Some x -> Printf.sprintf "after %.0f seconds" x);
	print "  UDP ping port:      %d\n" ad_hoc_controller;
	print "  UDP listening port: %d\n" ad_hoc_agent;
	print "  UDP name:           %S\n" ad_hoc_name;
);;


if Sys.os_type <> "Win32" then (
	Sys.catch_break true; (* This seems not to work on ocamlopt-compiled Windows programs *)
	Sys.set_signal Sys.sigpipe Sys.Signal_ignore; (* This is not needed on Windows, and probably wouldn't work anyway *)
);;


(* Agent-based file-finding *)
let find_file_with_md5 base file md5 =
	let on_dirs = split_on_dirs file in
	let rec check_for_file current_list = (
		let dir_now = List.fold_left (fun so_far gnu -> if so_far = "" then gnu else Filename.concat so_far gnu) "" current_list in
		let full_file = (if base = "" then dir_now else Filename.concat base dir_now) in (* The if statement here lets you use <base/> to mean exactly where the controller is using it *)
(*		print "Testing file \"%s\": " full_file;*)
		if is_file full_file then (
			if Digest.file full_file = md5 then (
				print "Testing file \"%s\": FOUND!\n" full_file;
				Some full_file
			) else (
				if base = "" then (
					print "Testing file \"%s\": MD5 does not match up (no base)\n" full_file;
					None
				) else (
					print "Testing file \"%s\": MD5 does not match up\n" full_file;
					match current_list with
					| hd :: tl -> check_for_file tl
					| [] -> (None)
				)
			)
		) else (
			if base = "" then (
				print "Testing file \"%s\": not a file (no base)\n" full_file;
				None
			) else (
				print "Testing file \"%s\": not a file\n" full_file;
				match current_list with
				| hd :: tl -> check_for_file tl
				| [] -> (None)
			)
		)
	) in
	check_for_file on_dirs
;;

(*******************************)
(* LISTEN FOR CONTROLLER PINGS *)
(*******************************)
(*
 * AGNTpp12NAME................
 * pp = port
 * 1 = 1st pass agents
 * 2 = 2nd pass agents
 * ... = MD5 of rest of string
 *)
let send_ping port = ( (* This port is the AGENT'S TCP port for the controller to connect to *)
	let str1 = "AGNT" ^ packn port ^ packC number_of_agents ^ packC (number_of_agents + number_of_agents_pad) ^ ad_hoc_name in
	let str = str1 ^ Digest.string str1 in
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.setsockopt sock Unix.SO_BROADCAST true;
	fun to_addr -> (
		let sent = Unix.sendto sock str 0 (String.length str) [] to_addr in
		if sent <> String.length str then (
			print "  Oops. Only sent %d out of %d bytes. I wonder why...\n" sent (String.length str)
		);
	)
);;

let listen_guts agent_connected_port = (
	let recv_sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
	Unix.bind recv_sock (Unix.ADDR_INET (Unix.inet_addr_any, ad_hoc_agent));

	print "Listener set up\n";

	let send_ping_to = send_ping agent_connected_port in

	(* The received string is "CONT" followed by MD5 of that exact string (53f6b3ace3aa40916de167636293ac80) *)
	(* OR (in the future) "CONT" followed by the 16-bit response port number, then the MD5 of those 6 bytes *)
	let recv_string = "CONTxx................" in
	while true do
		try
			let (got_bytes, from_addr) = Unix.recvfrom recv_sock recv_string 0 20 [] in
			match (from_addr, got_bytes) with
			| (Unix.ADDR_INET (from_ip,from_port), 20) when (String.sub recv_string 0 20 = "CONT\x53\xf6\xb3\xac\xe3\xaa\x40\x91\x6d\xe1\x67\x63\x62\x93\xac\x80") -> (
				(* OK *)
				print ~screen:false "Responding to ping from %s:%d\n" (Unix.string_of_inet_addr from_ip) from_port;
				send_ping_to (Unix.ADDR_INET (from_ip,ad_hoc_controller));
			)
			| (Unix.ADDR_INET (from_ip,from_port), 22) when (String.sub recv_string 0 4 = "CONT" && Digest.substring recv_string 0 6 = String.sub recv_string 6 16) -> (
				(* New (so far unused) form with the response port after "CONT" *)
				print ~screen:false "Responding to ping on port %d from %s:%d\n" (unpackn recv_string 4) (Unix.string_of_inet_addr from_ip) from_port;
				send_ping_to (Unix.ADDR_INET (from_ip, unpackn recv_string 4));
			)
			| _ -> (
				print "Listener threw out improper ping\n"
			)
		with
			x -> print "Listener failed: %S\n" (Printexc.to_string x)
	done
);;
(* On second thought, do this after connection, just in case I add the port range back again *)
(*
if !ad_hoc_ref then (
	ignore (Thread.create listen_guts ())
);;
*)

(***********************)
(* STALE FILE DELETION *)
(***********************)
let stale_guts () = (
	match stale with
	| None -> ()
	| Some s -> (
		let output_regex   = Str.regexp "^output [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]\\.mkv$" in
		let stats_regex    = Str.regexp "^stats [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]\\.txt\\(\\.temp\\)?$" in
		let statsold_regex = Str.regexp "^[0-9]+ [0-9]+ [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]\\.txt\\(\\.temp\\)?$" in
		let wake_up_time = max (min s 86400.0) 3600.0 in (* Do every 1 hour up to 1 day, depending on the stale parameter *)

		let rec do_this () = (
			let time = Unix.time () in
			print "Removing stale files from temp dir...\n";
			let dir_guts = Sys.readdir temp_dir in
			Array.iter (fun base ->
				try (
					let stats = Unix.LargeFile.stat (Filename.concat temp_dir base) in
					if time -. stats.Unix.LargeFile.st_mtime > s && stats.Unix.LargeFile.st_kind = Unix.S_REG then (
						(* File may be deleted; check to make sure it's the right kind of file *)
						if Str.string_match output_regex base 0 || Str.string_match stats_regex base 0 || Str.string_match statsold_regex base 0 then (
							(* BALEETED! *)
(*							print "STALE deleting file \"%s\"\n" base;*)
							Unix.unlink (Filename.concat temp_dir base)
						) else (
(*							print "STALE ignoring file \"%s\"\n" base*)
						)
					) else (
(*						print "STALE not time for \"%s\"\n" base*)
					)
				) with
					_ -> ()
			) dir_guts;

			Thread.delay wake_up_time;
			do_this ()
		) in
		do_this ()
	)
);;

ignore (Thread.create stale_guts ());;

(***************)
(* COMPRESSION *)
(***************)
let decompress = (
	let dehuff_y = Huff.compute_dehuff_table Huff.y_data 8 in
	let dehuff_u = Huff.compute_dehuff_table Huff.u_data 8 in

	let paeth a b c = (
		let p = a + b - c in
		let pa = abs (p - a) in
		let pb = abs (p - b) in
		let pc = abs (p - c) in
		if pa <= pb && pa <= pc then (
			a
		) else if pb <= pc then (
			b
		) else (
			c
		)
(*		a + b - c*)
	) in

	fun res_x res_y comp -> (
		match comp.[0] with
		| '\x00' -> (
			(* Uncompressed *)
			let frame_length = res_x * res_y in
			let out_string = String.sub comp 1 frame_length in

			(* MD5 *)
(*
			let controller_md5 = String.sub comp (frame_length + 1) 16 in
			let agent_md5 = Digest.string q in
			if controller_md5 <> agent_md5 then (
				print "MD5s do not match up! %s <> %s\n" (to_hex controller_md5) (to_hex agent_md5);
				let (write_name, write_me) = open_temp_file "MD5 error 00 (" ").txt" in
				Printf.fprintf write_me "%s <> %s.\nGot:\n%s\n\n\nDecompressed to:\n%s\n" (to_hex controller_md5) (to_hex agent_md5) (to_hex q) (to_hex q);
				close_out write_me;
			);
*)
			(* !MD5 *)

			out_string
		)
		| '\x01' -> (
			(* Paeth, Huffman encoded Y slice *)

			let out_string = String.create (res_x * res_y) in
			let in_buff = Huff.huff_buff_of_string ~start:1 comp in

			if true then (
				if true then (
					(* De-huff *)
					let val_bits_ref = ref (0,0) in
					for q = 0 to res_x * res_y - 1 do
						let (value,new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_y in
						out_string.[q] <- Char.chr value;
						val_bits_ref := new_val_bits
					done;

					(* De-Paeth *)
					Optimizations.undo_paeth out_string res_x 0
				) else (
(*
					let controller_md5 = String.sub comp (String.length comp - 16) 16 in
					let agent_md5 = Digest.string out_string in
*)
					let val_bits_ref = ref (0,0) in

					(* Do the upper-left pixel *)
					let (value,new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_y in
					out_string.[0] <- Char.chr value;
					val_bits_ref := new_val_bits;

					(* Now the top row *)
					let a_ref = ref value in
					for x = 1 to res_x - 1 do
						let (value,new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_y in
						let new_a = (value + !a_ref) land 0xFF in
						out_string.[x] <- Char.chr new_a;
						a_ref := new_a;
						val_bits_ref := new_val_bits;
					done;

					let a_ref = ref 0 in
					let c_ref = ref 0 in
					for y = 1 to res_y - 1 do
						let y_offset = y * res_x in
						let y_offset_prev = (y - 1) * res_x in

						(* First pixel in row *)
						let (value,new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_y in
						let b = Char.code out_string.[y_offset_prev] in
						let new_a = (value + b) land 0xFF in
						out_string.[y_offset] <- Char.chr new_a;
						val_bits_ref := new_val_bits;
						a_ref := new_a;
						c_ref := b;

						(* Rest of the row *)
						for x = 1 to res_x - 1 do
							let (value,new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_y in
							let b = Char.code out_string.[y_offset_prev + x] in
							let new_a = (value + paeth !a_ref b !c_ref) land 0xFF in
							out_string.[y_offset + x] <- Char.chr new_a;
							val_bits_ref := new_val_bits;
							a_ref := new_a;
							c_ref := b;
						done;
					done;
				)

			) else (
				let rec decode_frame x y parts = (
					if y >= res_y then (
						(* DONE! Nothing more to do *)
						()
					) else if x >= res_x then (
						(* Go to the next line *)
						decode_frame 0 (succ y) parts
					) else (
						(* Normal byte *)
						let (value,gnu_parts) = Huff.get_char in_buff parts dehuff_y in

						let a = (if x = 0 then 0 else Char.code out_string.[y * res_x + (x - 1)]) in
						let b = (if y = 0 then 0 else Char.code out_string.[(y - 1) * res_x + x]) in
						let c = (if x = 0 || y = 0 then 0 else Char.code out_string.[(y - 1) * res_x + (x - 1)]) in
						let p = (value + paeth a b c) land 0xFF in
						out_string.[y * res_x + x] <- Char.chr p;
						decode_frame (succ x) y gnu_parts
					)
				) in
				decode_frame 0 0 (0,0);
			);
			(* MD5 *)
(*
			let controller_md5 = String.sub comp (String.length comp - 16) 16 in
			let agent_md5 = Digest.string out_string in
			if controller_md5 <> agent_md5 then (
				print "MD5s do not match up! %s <> %s\n" (to_hex controller_md5) (to_hex agent_md5);
				let (write_name, write_me) = open_temp_file "MD5 error 01 (" ").txt" in
				Printf.fprintf write_me "%s <> %s.\nGot:\n%s\n\n\nDecompressed to:\n%s\n" (to_hex controller_md5) (to_hex agent_md5) (to_hex (String.sub comp 1 (String.length comp - 9))) (to_hex out_string);
				close_out write_me;
			);
*)
			(* !MD5 *)

			(* Now out_string should have an exact copy of the controller's input string *)
			out_string
		)
		| '\x02' -> (
			(* Paeth, Huffman encoded U/V slice *)

			let res_ux = res_x lsr 1 in
			let out_string = String.create (res_ux * res_y) in
			let in_buff = Huff.huff_buff_of_string ~start:1 comp in


			if true then (
				
				let val_bits_ref = ref (0,0) in
				
				(* UL *)
				let (value, new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_u in
				out_string.[0] <- Char.chr (value lxor 128);
				val_bits_ref := new_val_bits;
				
				(* Top row *)
				let a_ref = ref (value lxor 128) in
				for x = 1 to res_ux - 1 do
					let (value, new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_u in
					let new_a = (value + !a_ref) land 0xFF in
					out_string.[x] <- Char.chr new_a;
					a_ref := new_a;
					val_bits_ref := new_val_bits;
				done;
				
				let a_ref = ref 128 in
				let c_ref = ref 128 in
				for y = 1 to res_y - 1 do
					let y_offset = y * res_ux in
					let y_offset_prev = (y - 1) * res_ux in
					
					(* First pixel in row *)
					let (value, new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_u in
					let b = Char.code out_string.[y_offset_prev] in
					let new_a = (value + b) land 0xFF in
					out_string.[y_offset] <- Char.chr new_a;
					val_bits_ref := new_val_bits;
					a_ref := new_a;
					c_ref := b;
					
					(* Rest of the row *)
					for x = 1 to res_ux - 1 do
						let (value, new_val_bits) = Huff.get_char in_buff !val_bits_ref dehuff_u in
						let b = Char.code out_string.[y_offset_prev + x] in
						let new_a = (value + paeth !a_ref b !c_ref) land 0xFF in
						out_string.[y_offset + x] <- Char.chr new_a;
						val_bits_ref := new_val_bits;
						a_ref := new_a;
						c_ref := b;
					done;
				done;
				
			) else (
				let rec decode_frame x y parts = (
					if y >= res_y then (
						(* DONE! Nothing more to do *)
						()
					) else if x >= res_ux then (
						(* Go to the next line *)
						decode_frame 0 (succ y) parts
					) else (
						(* Normal byte *)
						let (value,gnu_parts) = Huff.get_char in_buff parts dehuff_u in

						let a = (if x = 0 then 128 else Char.code out_string.[y * res_ux + (x - 1)]) in
						let b = (if y = 0 then 128 else Char.code out_string.[(y - 1) * res_ux + x]) in
						let c = (if x = 0 || y = 0 then 128 else Char.code out_string.[(y - 1) * res_ux + (x - 1)]) in
						let p = (value + paeth a b c) land 0xFF in
						out_string.[y * res_ux + x] <- Char.chr p;
						decode_frame (succ x) y gnu_parts
					)
				) in
				decode_frame 0 0 (0,0);
			);


			(* MD5 *)
(*
			let controller_md5 = String.sub comp (String.length comp - 16) 16 in
			let agent_md5 = Digest.string out_string in
*)
(*
(*			let agent_md5 = "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00" in*)
			if controller_md5 <> agent_md5 then (
				print "MD5s do not match up! %s <> %s\n" (to_hex controller_md5) (to_hex agent_md5);
				let (write_name, write_me) = open_temp_file "MD5 error 02 (" ").txt" in
				Printf.fprintf write_me "%s <> %s.\nGot:\n%s\n\n\nDecompressed to:\n%s\n" (to_hex controller_md5) (to_hex agent_md5) (to_hex (String.sub comp 1 (String.length comp - 9))) (to_hex out_string);
				close_out write_me;
			);
*)
			(* !MD5 *)

			out_string
		)
	)
);;





(******************)
(* PER CONNECTION *)
(******************)
let do_per_connection sock buffer_mutex buffer_condition =
	(* The sock passed is the client sock! *)
	let print = (fun a -> print ~screen:true ~name:(string_of_int (Thread.id (Thread.self ())) ^ " ") a) in

	let recv () = recv sock in
	let send = send sock in

	(match recv () with
		| ("HELO","") -> ()
		| x -> raise (unexpected_packet x)
	);

	(match recv () with
		| ("SCOM",x) -> (send "SCO1" compression1_string; send "SCO2" compression2_string)
		| x -> raise (unexpected_packet x)
	);

	(**********)
	(* BUFFER *)
	(**********)
(*	let buffer = Prodcons.create 4 "" in (* I think I only need 2 of these... *)*)
	let buffer = Stringbuffer.create buffersize in
	let exception_ref = ref None in (* This stores any exceptions thrown by the buffer threads, so that the regular thread can pick it up *)

	let rec do_a_job () = (

		(*clear_buffer ();*)
		Stringbuffer.reset buffer;

		(* Grab the general settings *)
		let (favs_md5,favs_name) = (match recv () with
			| ("FAVS",x) when String.length x > 16 -> (String.sub x 0 16, String.sub x 16 (String.length x - 16))
			| ("FAVS",x) -> (raise (Failure (Printf.sprintf "Tag \"FAVS\" must be longer than 16 bytes (not %d)" (String.length x))))
			| x -> raise (unexpected_packet x)
		) in
		let (res_x,res_y,fps_n,fps_d) = (match recv () with
			| ("VNFO",x) when String.length x = 16 -> (unpackN x 0, unpackN x 4, unpackN x 8, unpackN x 12)
			| ("VNFO",x) -> (raise (Failure (Printf.sprintf "Tag \"VNFO\" must have length 16 (not %d)" (String.length x))))
			| x -> raise (unexpected_packet x)
		) in
		let fps_f = float_of_int fps_n /. float_of_int fps_d in
		let zone_string = (match recv () with
			| ("ZONE",x) -> x
			| x -> raise (unexpected_packet x)
		) in
		let zone_option = (if zone_string = "" then "" else "--zones " ^ zone_string) in

		let (f_from, f_to) = (match recv () with
			| ("RANG",x) when String.length x = 8 -> (unpackN x 0, unpackN x 4)
			| ("RANG",x) -> (raise (Failure (Printf.sprintf "Tag \"RANG\" must have length 8 (not %d)" (String.length x))))
			| x -> raise (unexpected_packet x)
		) in
		let f_frames = f_to - f_from + 1 in

		(match recv () with
			| ("1PAS", opts) -> (
				print "first pass frames %d to %d\n" f_from f_to;

(*				let stats_file = temp_file_name (Filename.concat temp_dir (Printf.sprintf "%d %d " f_from f_to)) ".txt" in*)
				let stats_file = temp_file_name (Filename.concat temp_dir "stats ") ".txt" in
				let stats_temp_file = stats_file ^ ".temp" in

				(* Attempt a base-based encoding *)
(*
				let do_base = (match base_option with
					| Some base -> find_file_with_md5 base favs_name favs_md5
					| None -> None
				) in
*)

				let do_base = (
					let rec iterate = function
						| [] -> None
						| base :: tl -> (
							match find_file_with_md5 base favs_name favs_md5 with
							| None -> iterate tl
							| Some x -> Some x
						)
					in
					iterate base_list
				) in

				(match do_base with
					| Some file -> (
						print "agent-based encoding\n";
						send "ENCD" "agent";

						(* Whip out a thread which sends a signal every so often *)
						let keep_going_ref = ref true in
						ignore (Thread.create (fun () ->
							while !keep_going_ref do
								send "INDY" ""; (* I'm Not Dead Yet! *)
								Thread.delay 10.0;
							done
						) ());

(*
						let x264_string = if nice = 0 then (
							Printf.sprintf "%s %s --fps %d/%d --pass 1 --stats \"%s\" %s --seek %d --frames %d -o %s \"%s\"" x264_exe opts fps_n fps_d stats_file zone_option f_from f_frames dev_null file
						) else (
							Printf.sprintf "nice -n %d %s %s --fps %d/%d --pass 1 --stats \"%s\" %s --seek %d --frames %d -o %s \"%s\"" nice x264_exe opts fps_n fps_d stats_file zone_option f_from f_frames dev_null file
						) in
*)

						let x264_string = match (nice, pipe) with
							| (0, false) -> (* Normal   *) Printf.sprintf "\"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s --seek %d --frames %d -o %s \"%s\"" x264_exe opts fps_n fps_d stats_file zone_option f_from f_frames dev_null file
							| (x, false) -> (* Niced    *) Printf.sprintf "nice -n %d \"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s --seek %d --frames %d -o %s \"%s\"" nice x264_exe opts fps_n fps_d stats_file zone_option f_from f_frames dev_null file
							| (0, true ) -> (* Piped    *) Printf.sprintf "avs2yuv -raw -seek %d -frames %d -o - \"%s\" | \"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" f_from f_frames file x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
							| (x, true ) -> (* NicePipe *) Printf.sprintf "nice -n %d avs2yuv -raw -seek %d -frames %d -o - \"%s\" | nice -n %d \"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" nice f_from f_frames file nice x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
						in

						print "doing the following:\n  %s\n" x264_string;

(*
						print "disregard that, let's throw an exception!\n";
						failwith "death and destruction";
*)

						let exited = Sys.command x264_string in
						print "exited %d\n" exited;
						
						if exited <> 0 then (failwith (Printf.sprintf "ERROR: nonzero return code from x264 (%d)" exited));
						
						keep_going_ref := false; (* Kill the heartbeat thread *)

						send "DONE" "";
						(* End agent-based encoding *)
					)
					| None -> (
						print "controller-based encoding\n";
						send "ENCD" "controller";

						(* Pass it to x264! *)
						let x264_string = if nice = 0 then (
							(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
							Printf.sprintf "\"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
						) else (
							Printf.sprintf "nice -n %d \"%s\" %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" nice x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
						) in
						print "doing the following:\n  %s\n" x264_string;
						let put = Unix.open_process_out x264_string in
(*
						let buffer_thread = Thread.create (fun () ->
							let rec get_frame () = (match recv () with
								| ("FRAM","") -> (print "got a null frame. Ignoring...\n"; get_frame ()) (* I don't think this ever happens, but just in case it does, I don't want to screw up the buffer *)
								| ("FRAM",x) -> (Prodcons.put buffer x; get_frame ())
								| ("ZFRM",x) -> (* Compressed frame! *) (Prodcons.put buffer (decompress res_x res_y x); get_frame ())
								| ("EOF!","") -> (print "putter got an EOF\n"; Prodcons.put buffer "")
								| x -> raise (unexpected_packet x)
							) in
							try
								get_frame ()
							with
								x -> (print "recv thread got exception; killing listener thread\n"; exception_ref := Some x; Prodcons.put buffer "")
						) () in
*)
						let buffer_thread = Thread.create (fun () ->
							let rec get_frame () = (match recv () with
								| ("FRAM",x)  -> (Stringbuffer.write_string buffer x; get_frame ())
								| ("ZFRM",x)  -> (Stringbuffer.write_string buffer (decompress res_x res_y x); get_frame ())
								| ("EOF!","") -> (print "putter got an EOF\n"; Stringbuffer.close buffer)
								| x -> raise (unexpected_packet x)
							) in
							try
								get_frame ()
							with
								x -> (print "recv thread got exception; killing listener thread\n"; exception_ref := Some x; Stringbuffer.close buffer)
						) () in

						(try
(*
							let rec add_frame () = (
								match (Prodcons.get buffer, !exception_ref) with
								| (_,Some x) -> (raise x)
								| ("",_) -> (print "getter got an EOF\n")
								| (x,_) -> (output_string put x; add_frame ())
							) in
							add_frame ();
*)
							let put_str = String.create 4096 in
							let rec add_frame () = (
								match (Stringbuffer.read buffer put_str 0 4096, !exception_ref) with
								| (_,Some x) -> raise x
								| (0,_) -> (print "getter got an EOF\n")
								| (x,_) -> (output put put_str 0 x; add_frame ())
							) in
							add_frame ();

							flush put;
						with
						| x -> (
							(* Close the process if something bad happens *)
							ignore (Unix.close_process_out put);
							raise x
						));

						print "Waiting for buffer thread to croak\n";
						Thread.join buffer_thread;

						(match (Unix.close_process_out put) with
							| Unix.WEXITED x -> (print "exited %d\n" x; if x <> 0 then (failwith (Printf.sprintf "ERROR: x264 returned nonzero (%d)" x)))
							| Unix.WSIGNALED x -> (print "signaled %d?\n" x; failwith (Printf.sprintf "ERROR: x264 exited with signal %d" x))
							| Unix.WSTOPPED x -> (print "stopped %d\n" x; failwith (Printf.sprintf "ERROR: x264 stopped with signal %d" x))
						);

						(* End controller-based encoding *)
					)
				);
				print "done encoding\n";

				(* Output the stats file *)
				let stats_handle = if is_file stats_temp_file then (
					(* Sometimes the file is not renamed, for some bizarre reason *)
					open_in stats_temp_file
				) else (
					open_in stats_file
				) in
				(try while true do
					let line = input_line stats_handle in
					send "STAT" line;
				done with
				| End_of_file -> (send "EOF!" "")
				| e -> (close_in stats_handle; raise e)
				);
				close_in stats_handle;
				(try
					Sys.remove stats_file; (* BALEETED! *)
				with
					Sys_error x -> ()
				);
				(try
					Sys.remove stats_temp_file; (* BALEETED! *)
				with
					Sys_error x -> ()
				);
			)
			| ("2PAS", opts) -> (
				print "2nd pass frames %d to %d\n" f_from f_to;

				let target_bitrate = (match recv () with
					| ("TBIT",x) -> int_of_string x
					| x -> raise (unexpected_packet x)
				) in

				let (temp_stats_name, temp_stats_handle) = open_temp_file (Filename.concat temp_dir "stats ") ".txt" in

				(try
					let rec recv_stats () = (
						match recv () with
						| ("STAT", x) -> (
							Printf.fprintf temp_stats_handle "%s\n" x;
							recv_stats ()
						)
						| ("EOF!","") -> (print "done recieving stats\n")
						| x -> raise (unexpected_packet x)
					) in
					recv_stats ();
				with
					e -> (close_out temp_stats_handle; raise e)
				);
				close_out temp_stats_handle;

				let mkv_file = (
					let (file, handle) = open_temp_file ~mode:[Open_binary] (Filename.concat temp_dir "output ") ".mkv" in
					close_out handle;
					file
				) in

				(* Attempt agent-based encoding *)
(*
				let do_base = (match base_option with
					| Some base -> find_file_with_md5 base favs_name favs_md5
					| None -> None
				) in
*)

				let do_base = (
					let rec iterate = function
						| [] -> None
						| base :: tl -> (
							match find_file_with_md5 base favs_name favs_md5 with
							| None -> iterate tl
							| Some x -> Some x
						)
					in
					iterate base_list
				) in

				(match do_base with
					| Some file -> (
						print "agent-based encoding\n";
						send "ENCD" "agent";

						(* Whip out a thread which sends a signal every so often *)
						let keep_going_ref = ref true in
						let heartbeat = Thread.create (fun () ->
							while !keep_going_ref do
								send "INDY" "";
								Thread.delay 10.0;
							done
						) () in

(*
						let x264_string = if nice = 0 then (
							Printf.sprintf "%s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s --seek %d --frames %d -o \"%s\" \"%s\"" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option f_from f_frames mkv_file file
						) else (
							Printf.sprintf "nice -n %d %s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s --seek %d --frames %d -o \"%s\" \"%s\"" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option f_from f_frames mkv_file file
						) in
*)

						let x264_string = match (nice, pipe) with
							| (0, false) -> (* Normal   *) Printf.sprintf "\"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s --seek %d --frames %d -o \"%s\" \"%s\"" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option f_from f_frames mkv_file file
							| (x, false) -> (* Niced    *) Printf.sprintf "nice -n %d \"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s --seek %d --frames %d -o \"%s\" \"%s\"" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option f_from f_frames mkv_file file
							| (0, true ) -> (* Piped    *) Printf.sprintf "avs2yuv -raw -seek %d -frames %d -o - \"%s\" | \"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" f_from f_frames file x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
							| (x, true ) -> (* NicePipe *) Printf.sprintf "nice -n %d avs2yuv -raw -seek %d -frames %d -o - \"%s\" | nice -n %d \"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" nice f_from f_frames file nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
						in

						print "doing the following:\n  %s\n" x264_string;
						let exited = Sys.command x264_string in
						print "exited %d\n" exited;

						if exited <> 0 then (failwith (Printf.sprintf "ERROR: nonzero return code from x264 (%d)" exited));

						keep_going_ref := false; (* Kill the heartbeat thread *)

						send "DONE" "";
						(* End agent-based encoding *)
					)
					| None -> (
						print "controller-based encoding\n";
						send "ENCD" "controller";

						(* Pass it to x264! *)
						let x264_string = if nice = 0 then (
							(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
							Printf.sprintf "\"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
						) else (
							Printf.sprintf "nice -n %d \"%s\" %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
						) in
						print "doing the following:\n  %s\n" x264_string;
						let put = Unix.open_process_out x264_string in
(*
						let buffer_thread = Thread.create (fun () ->
							let rec get_frame () = (match recv () with
								| ("FRAM","") -> (print "got a null frame. Ignoring...\n"; get_frame ())
								| ("FRAM",x) -> (Prodcons.put buffer x; get_frame ())
								| ("ZFRM",x) -> (Prodcons.put buffer (decompress res_x res_y x); get_frame ())
								| ("EOF!","") -> (print "putter got an EOF\n"; Prodcons.put buffer "")
								| x -> raise (unexpected_packet x)
							) in
							try
								get_frame ()
							with
								x -> (print "recv thread got exception \"%s\"; killing listener thread\n" (Printexc.to_string x); exception_ref := Some x; Prodcons.put buffer "")
						) () in
*)
						let buffer_thread = Thread.create (fun () ->
							let rec get_frame () = (match recv () with
								| ("FRAM",x)  -> (Stringbuffer.write_string buffer x; get_frame ())
								| ("ZFRM",x)  -> (Stringbuffer.write_string buffer (decompress res_x res_y x); get_frame ())
								| ("EOF!","") -> (print "putter got an EOF\n"; Stringbuffer.close buffer)
								| x -> raise (unexpected_packet x)
							) in
							try
								get_frame ()
							with
								x -> (print "recv thread got exception; killing listener thread\n"; exception_ref := Some x; Stringbuffer.close buffer)
						) () in

						(try
(*
							let rec add_frame () = (
								match (Prodcons.get buffer, !exception_ref) with
								| (_,Some x) -> (raise x)
								| ("",_) -> (print "getter got an EOF\n")
								| (x,_) -> (output_string put x; add_frame ())
							) in
							add_frame ();
*)
							let put_str = String.create 4096 in
							let rec add_frame () = (
								match (Stringbuffer.read buffer put_str 0 4096, !exception_ref) with
								| (_,Some x) -> raise x
								| (0,_) -> (print "getter got an EOF\n")
								| (x,_) -> (output put put_str 0 x; add_frame ())
							) in
							add_frame ();

							flush put;
						with
							(* This should fix some problems with too many open files... *)
							e -> (ignore (Unix.close_process_out put); raise e)
						);
						
						print "Waiting for buffer thread to croak\n";
						Thread.join buffer_thread;
						
						(match (Unix.close_process_out put) with
							| Unix.WEXITED x -> (print "exited %d\n" x; if x <> 0 then (failwith (Printf.sprintf "ERROR: x264 returned nonzero (%d)" x)))
							| Unix.WSIGNALED x -> (print "signaled %d?\n" x; failwith (Printf.sprintf "ERROR: x264 exited with signal %d" x))
							| Unix.WSTOPPED x -> (print "stopped %d\n" x; failwith (Printf.sprintf "ERROR: x264 stopped with signal %d" x))
						);

						(* End controller-based encoding *)
					)
				);
				print "done encoding\n";

				(* Now output the stats file *)
				let stats_handle = open_in temp_stats_name in
				(try while true do
					let line = input_line stats_handle in
					send "STAT" line;
				done with
				| End_of_file -> (send "EOF!" "")
				| e -> (close_in stats_handle; raise e)
				);
				close_in stats_handle;
				(try
					Sys.remove temp_stats_name;  (* Don't need that anymore *)
				with
					Sys_error x -> (
						Unix.sleep 2;
						(try
							Sys.remove temp_stats_name
						with
							Sys_error x -> ()
						)
					)
				);

				(* Now output the MKV *)
				print "sending MKV\n";
				let mkv_handle = open_in_bin mkv_file in
				(try
					let read_and_output = (
						let temp_string = String.create 4096 in
						let rec a () = (
							let actually_read = input mkv_handle temp_string 0 4096 in
							if actually_read = 4096 then (
								send "MOOV" temp_string;
								a ()
							) else if actually_read = 0 then (
								(* END! *)
								send "EOF!" "";
							) else (
								send "MOOV" (String.sub temp_string 0 actually_read);
								a ()
							)
						) in
						a
					) in
					read_and_output ();
				with
					e -> (close_in mkv_handle; raise e)
				);
				close_in mkv_handle;
				(try
					Sys.remove mkv_file;
				with
					Sys_error x -> (
						Unix.sleep 2;
						(try
							Sys.remove mkv_file
						with
							Sys_error x -> ()
						)
					)
				);
			)
			| x -> raise (unexpected_packet x)
		);
		print "done with job\n";
		do_a_job () (* AGAIN! *)
	) in
	do_a_job ()
;;
let catch_thread (sock,mutex,condition) =
	(try
		do_per_connection sock mutex condition
	with
	| Unix.Unix_error (x,y,z) -> (print "Thread received exception (%S,%s,%s)\n" (Unix.error_message x) y z)
	| x -> (print "Thread received exception %S\n" (Printexc.to_string x))
	);
	print "closing socket\n";
	Unix.close sock;
	print "socket closed\n";
;;

let set_up_server () =
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	let bound_port = (
		let rec find_port p = (
			if p > agent_port_to then find_port agent_port_from else (
				print "trying port %d\n" p;
				(try
					Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, p));
					p
				with
					_ -> find_port (succ p)
				)
			)
		) in
		find_port agent_port_from
	) in

(*	Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, bound_port));*)
	print "bound to port %d\n" bound_port;
	print "listening...\n";
	Unix.listen sock (number_of_agents + number_of_agents_pad); (* The parameter to listen doesn't seem to work, but I might as well set it correctly anyway *)

	(* Start up the UDP listener thread to respond to ad-hoc connection requests *)
	if !ad_hoc_ref && ad_hoc_controller <> 0 then (
		(* Send out a startup ping *)
		(match Sys.os_type with
			| "Win32" -> send_ping bound_port (Unix.ADDR_INET (Obj.magic "\255\255\255\255", ad_hoc_controller))
			| _ -> send_ping bound_port (Unix.ADDR_INET (Unix.inet_addr_of_string "255.255.255.255", ad_hoc_controller))
		);
		ignore (Thread.create listen_guts bound_port)
	);

	while true do
		print "working!\n";

		let (client_sock, other_addr) = Unix.accept sock in
		let (other_ip, other_port) = match other_addr with
			| Unix.ADDR_INET (x,y) -> ((Unix.string_of_inet_addr x), y)
			| _ -> ("UNIX???", 0)
		in
		print "Got a connection from %s:%d\n" other_ip other_port;

		let mutex = Mutex.create () in
		let condition = Condition.create () in

		ignore (Thread.create catch_thread (client_sock,mutex,condition))
	done
;;
set_up_server ();;
