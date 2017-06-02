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
open Pack;;
open Net;;
open Matroska;;

let debug_file = true;;

let config_ref = ref "config.xml";;

let arg_parse = Arg.align [
(*	("--port", Arg.Set_int port_ref, " Listening port (15086)");*)
	("--config", Arg.Set_string config_ref,      " Location of the config XML file (\"config.xml\")");
];;

(* Set the config file to whatever the last argument was *)
Arg.parse arg_parse (fun x -> config_ref := x) (Printf.sprintf "Distributed encoding AGENT (version %s)" version);;

let parse_config_file file = (
	let file = match (search_for_file file, search_for_file "config.xml") with
		| (Some x, _) -> x
		| (None, Some y) -> (print "WARNING: Config file \"%s\" does not exist; using \"%s\" instead\n" file y; y)
		| (None, None) -> (print "ERROR: No configuration file found; exiting\n"; exit 2)
	in
	print "Using config file \"%s\"\n" file;

	let temp_dir_ref = ref "" in
	let x264_exe_ref = ref "x264" in
	let port_from_ref = ref 50700 in
	let port_to_ref = ref 50710 in
	let nice_ref = ref 0 in
	let number_ref = ref 1 in

	let parse_config = (function
		| Xml.Element ("temp", _, [Xml.PCData x]) -> temp_dir_ref := x
		| Xml.Element ("port", [("from",x); ("to",y)], []) -> (port_from_ref := int_of_string x; port_to_ref := int_of_string y)
		| Xml.Element ("x264", _, [Xml.PCData x]) -> x264_exe_ref := x
		| Xml.Element ("nice", _, [Xml.PCData x]) -> nice_ref := int_of_string x
		| Xml.Element ("number",_,[Xml.PCData x]) -> number_ref := int_of_string x
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

	(!temp_dir_ref, !port_from_ref, !port_to_ref, !x264_exe_ref, !nice_ref, !number_ref)

);;

(* Parse the config file *)
let (temp_dir, port_from, port_to, x264_exe, nice, number_of_agents) = parse_config_file !config_ref;;
if debug_file then print " Temp dir: \"%s\"\n Ports: %d - %d\n x264: \"%s\"\n nice: %d\n agents: %d\n" temp_dir port_from port_to x264_exe nice number_of_agents;;


if Sys.os_type <> "Win32" then (
	Sys.catch_break true; (* This seems not to work on ocamlopt-compiled Windows programs *)
	Sys.set_signal Sys.sigpipe Sys.Signal_ignore; (* This is not needed on Windows, and probably wouldn't work anyway *)
);;

(*********************)
(* Try this again... *)
(*********************)
let do_per_connection sock buffer_mutex buffer_condition =
	(* The sock passed is the client sock! *)
	let print = (fun a -> print ~screen:false ~name:(string_of_int (Thread.id (Thread.self ())) ^ " ") a) in

	let recv () = recv sock in
	let send = send sock in

	(match recv () with
		| ("HELO","") -> ()
		| x -> raise (unexpected_packet x)
	);

	(**********)
	(* BUFFER *)
	(**********)
	let buffer = Prodcons.create 4 "" in

(*
	let buffer_length = 4 in
	let buffer = {
		buf_array = Array.make buffer_length (String.create max_string_length);
		buf_ok = Array.make buffer_length 0;
		buf_get = 0;
		buf_put = 0;
		buf_eof = false;
	} in
(*
	let buffer_mutex = Mutex.create () in
	let buffer_condition = Condition.create () in
*)
	print "created buffer of length %dx%d\n" buffer_length max_string_length;

let qbert = true in

	let clear_buffer () = (
		Mutex.lock buffer_mutex;
if qbert then		print "clear locked buffer mutex\n";
		Array.fill buffer.buf_ok 0 buffer_length 0;
		buffer.buf_get <- 0;
		buffer.buf_put <- 0;
		buffer.buf_eof <- false;
if qbert then print "clear broadcasting\n";
		Condition.signal buffer_condition;
if qbert then		print "clear unlocked buffer mutex\n";
		Mutex.unlock buffer_mutex;
	) in
	let rec receive_until_eof () = (
		(match recv () with
			| ("FRAM",x) -> (
				Mutex.lock buffer_mutex;
if qbert then				print "receive locked buffer mutex\n";
				while buffer.buf_ok.(buffer.buf_put) <> 0 do
if qbert then print "next writing bucket (%d) is not empty (%d); spinning\n" buffer.buf_put buffer.buf_ok.(buffer.buf_put);
					Condition.wait buffer_condition buffer_mutex;
if qbert then print "receive going\n";
				done;
				let len = String.length x in
				String.blit x 0 buffer.buf_array.(buffer.buf_put) 0 len;
				buffer.buf_ok.(buffer.buf_put) <- len;
if qbert then print "added %d bytes to bucket %d\n" len buffer.buf_put;
				buffer.buf_put <- succ buffer.buf_put mod buffer_length;
if qbert then print "receive broadcasting\n";
				Condition.signal buffer_condition;
if qbert then 				print "receive unlocked buffer mutex\n";
				Mutex.unlock buffer_mutex;

				receive_until_eof ()
			)
			| ("EOF!","") -> (
				Mutex.lock buffer_mutex;
				buffer.buf_eof <- true;
if qbert then print "receive broadcasting (EOF)\n";
				Condition.signal buffer_condition;
				Mutex.unlock buffer_mutex;
			)
			| x -> (raise (unexpected_packet x))
		)
	) in
	let rec write_until_eof handle = (
		Mutex.lock buffer_mutex;
if qbert then 		print "write locked buffer mutex\n";
		while (buffer.buf_ok.(buffer.buf_get) = 0 && not buffer.buf_eof) do
if qbert then print "next reading bucket (%d) is empty; spinning\n" buffer.buf_get;
			Condition.wait buffer_condition buffer_mutex;
if qbert then print "write going\n";
		done;
		let did_stuff = if buffer.buf_ok.(buffer.buf_get) = 0 then (
			false
		) else (
			output handle buffer.buf_array.(buffer.buf_get) 0 buffer.buf_ok.(buffer.buf_get);
if qbert then print "grabbed %d bytes from bucket %d\n" buffer.buf_ok.(buffer.buf_get) buffer.buf_get;
			buffer.buf_ok.(buffer.buf_get) <- 0;
			buffer.buf_get <- succ buffer.buf_get mod buffer_length;
			true
		) in
if qbert then print "write broadcasting\n";
		Condition.signal buffer_condition;
if qbert then 		print "write unlocked buffer mutex\n";
		Mutex.unlock buffer_mutex;
		
		if did_stuff then (
			write_until_eof handle
		) else (
			print "write stuff died\n";
			()
		)
	) in
*)

	let rec do_a_job () = (

		(*clear_buffer ();*)

		(* Grab the general settings *)
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

				let stats_file = temp_file_name (Filename.concat temp_dir (Printf.sprintf "%d %d " f_from f_to)) ".txt" in
				let stats_temp_file = stats_file ^ ".temp" in

				(* Pass it to x264! *)
				let x264_string = if nice = 0 then (
					(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
					Printf.sprintf "%s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
				) else (
					Printf.sprintf "nice -n %d %s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" nice x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
				) in
				print "doing the following:\n  %s\n" x264_string;
				let put = Unix.open_process_out x264_string in

(*
				let buffer_thread = Thread.create (fun () ->
					Mutex.lock buffer_mutex;
					while true do
						print "waiting...\n";
						Condition.wait buffer_condition buffer_mutex;
						print "going\n";
					done;
					Mutex.unlock buffer_mutex;
					
				) () in

				if Thread.id (Thread.self ()) = 1 then (
					while true do
						print "broadcasting...\n";
						Condition.signal buffer_condition;
						Unix.sleep 3;
					done
				) else (
					while true do
						print "broadcasting...\n";
						Condition.signal buffer_condition;
						Unix.sleep 2;
					done;
					Unix.sleep 10000
				);
*)
(*
				let buffer_thread = Thread.create (fun x -> 
		print "spawned from %d\n" x;
		let rec receive_until_eof () = (match recv () with
			| ("FRAM",x) -> (
				Mutex.lock buffer_mutex;
if qbert then				print "receive locked buffer mutex\n";
				while buffer.buf_ok.(buffer.buf_put) <> 0 do
if qbert then print "next writing bucket (%d) is not empty (%d); spinning\n" buffer.buf_put buffer.buf_ok.(buffer.buf_put);
					Condition.wait buffer_condition buffer_mutex;
if qbert then print "receive going\n";
				done;
				let len = String.length x in
				String.blit x 0 buffer.buf_array.(buffer.buf_put) 0 len;
				buffer.buf_ok.(buffer.buf_put) <- len;
if qbert then print "added %d bytes to bucket %d\n" len buffer.buf_put;
				buffer.buf_put <- succ buffer.buf_put mod buffer_length;
if qbert then print "receive broadcasting\n";
				Condition.signal buffer_condition;
if qbert then 				print "receive unlocked buffer mutex\n";
				Mutex.unlock buffer_mutex;

				receive_until_eof ()
			)
			| ("EOF!","") -> (
				Mutex.lock buffer_mutex;
				buffer.buf_eof <- true;
if qbert then print "receive broadcasting (EOF)\n";
				Condition.signal buffer_condition;
				Mutex.unlock buffer_mutex;
			)
			| x -> (raise (unexpected_packet x))
		) in
		receive_until_eof ()

				) (Thread.id (Thread.self ())) in
*)
(*
let bthread2 = Thread.create (fun x ->
	print "spawned from %d\n" x;
	while true do
(*		Mutex.lock buffer_mutex;*)
		print "Broadcasting...\n";
		Condition.broadcast buffer_condition;
(*		Mutex.unlock buffer_mutex;*)
		Unix.sleep 10;
	done;
) (Thread.id (Thread.self ())) in
*)

				let buffer_thread = Thread.create (fun () ->
					let rec get_frame () = (match recv () with
						| ("FRAM","") -> (print "got a null frame. Ignoring...\n"; get_frame ())
						| ("FRAM",x) -> (Prodcons.put buffer x; get_frame ())
						| ("EOF!","") -> (print "putter got an EOF\n"; Prodcons.put buffer "")
						| x -> raise (unexpected_packet x)
					) in
					get_frame ()
				) () in

				(try

					let rec add_frame () = (
						match Prodcons.get buffer with
						| "" -> (print "getter got an EOF\n")
						| x -> (output_string put x; add_frame ())
					) in
					add_frame ();

(*
					let rec add_frame () = (
						match recv () with
						| ("FRAM",x)  -> (output_string put x; add_frame ())
						| ("EOF!","") -> ()
						| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
					) in
					add_frame ();
					write_until_eof put;
*)
					flush put;
				with
				| x -> (
					(* Close the process if something bad happens *)
					ignore (Unix.close_process_out put);
					raise x
				));
(*
				print "Waiting for buffer thread to croak\n";
				Thread.join buffer_thread;
*)
				(match (Unix.close_process_out put) with
					| Unix.WEXITED x -> print "exited %d\n" x
					| Unix.WSIGNALED x -> print "signaled %d?\n" x
					| Unix.WSTOPPED x -> print "stopped %d\n" x
				);

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

				(* Pass it to x264! *)
				let x264_string = if nice = 0 then (
					(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
					Printf.sprintf "%s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
				) else (
					Printf.sprintf "nice -n %d %s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
				) in
				print "doing the following:\n  %s\n" x264_string;
				let put = Unix.open_process_out x264_string in
				(try
					let rec add_frame () = (
						match recv () with
						| ("FRAM",x)  -> (output_string put x; add_frame ())
						| ("EOF!","") -> ()
						| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
					) in
					add_frame ();
					flush put;
				with
					(* This should fix some problems with too many open files... *)
					e -> (ignore (Unix.close_process_out put); raise e)
				);
				(match (Unix.close_process_out put) with
					| Unix.WEXITED x -> print "exited %d\n" x
					| Unix.WSIGNALED x -> print "signaled %d?\n" x
					| Unix.WSTOPPED x -> print "stopped %d\n" x
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
	Unix.close sock
;;

let set_up_server () =
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	let bound_port = (
		let rec find_port p = (
			if p > port_to then find_port port_from else (
				print "trying port %d\n" p;
				(try
					Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, p));
					p
				with
					_ -> find_port (succ p)
				)
			)
		) in
		find_port port_from
	) in
	print "bound to port %d\n" bound_port;
	print "listening...\n";
	Unix.listen sock number_of_agents; (* The parameter to listen doesn't seem to work, but I might as well set it correctly anyway *)

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



















(*********************)
(* CONNECTION LOCKER *)
(*********************)
(* only_one_connection () should be initialized per agent thread *)
(* Then the resultant function should be used per connection *)
(*
let only_one_connection num =
	let caller_id = Thread.id (Thread.self ()) in
	print "Thread #%d made a new thread batch\n" caller_id;
	let mute = Mutex.create () in
	let num_working_ref = ref 0 in

	fun (client_sock, func, arguments) -> (
		print "Made thread #%d (%d)\n" (Thread.id (Thread.self ())) caller_id;
		if !num_working_ref >= num then (
			Unix.close client_sock;
			print "Thread #%d no good! (%d) with %d\n" (Thread.id (Thread.self ())) caller_id;
		) else (
			Mutex.lock mute;
			is_working_ref := true;
			Mutex.unlock mute;
			(try
				func arguments;
				print "Killed off thread #%d (%d)\n" (Thread.id (Thread.self ())) caller_id;
				Mutex.lock mute;
				decr num_working_ref;
				Mutex.unlock mute;
			with
				x -> (
					Mutex.lock mute;
					decr num_working_ref;
					Mutex.unlock mute;
					raise x
				)
			)
		)
	)
;;
*)
(*****************************)
(* AGENT GUTS PER CONNECTION *)
(*****************************)
let agent_connection_guts (client_sock, other_addr, agents_working_mutex, agents_working_ref) =
	let id = Thread.id (Thread.self ()) in
	let print = (fun a -> print ~name:(string_of_int id ^ " ") a) in
	(try
		(match other_addr with
			| Unix.ADDR_INET (x,y) -> (print "got a connection from %s:%d\n" (Unix.string_of_inet_addr x) y)
			| _ -> (print "got a Unix connection???\n"; failwith "weird connection")
		);
		let recv () = recv client_sock in
		let send = send client_sock in

		(* Make sure the sender is sane *)
		(match recv () with
			| ("HELO", "") -> ()
			| x -> raise (unexpected_packet x)
		);

		(* Each pass needs at least pass options, zone options, and a range. 2nd pass also needs the 1st pass stats file *)
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

			print "1st pass\n";

			let stats_file = temp_file_name (Filename.concat temp_dir (Printf.sprintf "%d %d " f_from f_to)) ".txt" in
			let stats_temp_file = stats_file ^ ".temp" in

			(* Pass it to x264! *)
			let x264_string = if nice = 0 then (
				(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
				Printf.sprintf "%s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
			) else (
				Printf.sprintf "nice -n %d %s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" nice x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
			) in
			print "doing the following:\n  %s\n" x264_string;
			let put = Unix.open_process_out x264_string in
			(try
				let rec add_frame () = (
					match recv () with
					| ("FRAM",x)  -> (output_string put x; add_frame ())
					| ("EOF!","") -> ()
					| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
				) in
				add_frame ();
				flush put;
			with
			| x -> (
				(* Close the process if something bad happens *)
				ignore (Unix.close_process_out put);
				raise x
			));

			(match (Unix.close_process_out put) with
				| Unix.WEXITED x -> print "exited %d\n" x
				| Unix.WSIGNALED x -> print "signaled %d?\n" x
				| Unix.WSTOPPED x -> print "stopped %d\n" x
			);

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

			print "2nd pass\n";

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
				let (file, handle) = open_temp_file (Filename.concat temp_dir "output ") ".mkv" in
				close_out handle;
				file
			) in

			(* Pass it to x264! *)
			let x264_string = if nice = 0 then (
				(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
				Printf.sprintf "%s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
			) else (
				Printf.sprintf "nice -n %d %s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
			) in
			print "doing the following:\n  %s\n" x264_string;
			let put = Unix.open_process_out x264_string in
			(try
				let rec add_frame () = (
					match recv () with
					| ("FRAM",x)  -> (output_string put x; add_frame ())
					| ("EOF!","") -> ()
					| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
				) in
				add_frame ();
				flush put;
			with
				(* This should fix some problems with too many open files... *)
				e -> (ignore (Unix.close_process_out put); raise e)
			);
			(match (Unix.close_process_out put) with
				| Unix.WEXITED x -> print "exited %d\n" x
				| Unix.WSIGNALED x -> print "signaled %d?\n" x
				| Unix.WSTOPPED x -> print "stopped %d\n" x
			);
			print "DONE\n";

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
		| x -> raise (unexpected_packet x);
		); (* Big match statement *)
		print "finished OK; shutting down socket\n";
	with
		x -> (print "died with %s\n" (Printexc.to_string x))
	);
	Unix.close client_sock;

	Mutex.lock agents_working_mutex;
	decr agents_working_ref;
	print "job ended; there are now %d agents working\n" !agents_working_ref;
	Mutex.unlock agents_working_mutex;
;;










(**********************************
(* Set up the agent threads *)
(*
let agent_guts (id, port_from, port_to) = (
	let print = (fun a -> print ~name:(string_of_int id ^ " ") a) in
	let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	let bound_port = (
		let rec find_port p = (
			if p > port_to then find_port port_from else (
				print "trying port %d\n" p;
				(try
					Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, p));
					p
				with
					_ -> find_port (succ p)
				)
			)
		) in
		find_port port_from
	) in
	print "bound to port %d\n" bound_port;
	print "listening...\n";
	Unix.listen sock 1;

	let run_once = only_one_connection () in

	while true do
		print "working!\n";

		let (client_sock, other_addr) = Unix.accept sock in

		try (

			(* STUFF GOES HERE *)
			(*run_once client_sock agent_connection_guts (client_sock, other_addr, id);*)
			ignore (Thread.create run_once (client_sock, agent_connection_guts, (client_sock, other_addr, id)));

		) with
		| Sys.Break -> (print "got a break! exiting...\n"; raise Sys.Break)
		| Unix.Unix_error (a,b,c) -> (print "croaked with exception \"Unix.Unix_error(%S,%S,%S)\"; restarting\n" (Unix.error_message a) b c; Unix.close client_sock)
		| x -> (print "agent croaked with exception %S; restarting\n" (Printexc.to_string x); Unix.close client_sock)
	done;
);;
let agent_threads = Array.init number_of_agents (fun idm1 -> Thread.create agent_guts (succ idm1, port_from, port_to));;
Array.iter (fun x -> Thread.join x) agent_threads;;
*)




(* Do the threading at the connection level! *)
(* Because it seems to b0rk if I do it per agent *)
let connect_and_do () =
	let agents_working_mutex = Mutex.create () in
	let agents_working_ref = ref 0 in

	let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
	let bound_port = (
		let rec find_port p = (
			if p > port_to then find_port port_from else (
				print "trying port %d\n" p;
				(try
					Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, p));
					p
				with
					_ -> find_port (succ p)
				)
			)
		) in
		find_port port_from
	) in
	print "bound to port %d\n" bound_port;
	print "listening...\n";
	Unix.listen sock number_of_agents; (* The parameter to listen doesn't seem to work, but I might as well set it correctly anyway *)

	while true do
		print "working!\n";

		let (client_sock, other_addr) = Unix.accept sock in

		let (other_ip, other_port) = match other_addr with
			| Unix.ADDR_INET (x,y) -> ((Unix.string_of_inet_addr x), y)
			| _ -> ("UNIX???", 0)
		in
		print "Got a connection from %s:%d\n" other_ip other_port;

		Mutex.lock agents_working_mutex;
		if !agents_working_ref >= number_of_agents then (
			Mutex.unlock agents_working_mutex;
			print "Too many connections; kick out %s:%d\n" other_ip other_port;
			Unix.close client_sock;
		) else (
			incr agents_working_ref;
			print "There are now %d agents working\n" !agents_working_ref;
			Mutex.unlock agents_working_mutex;

			(try
				ignore (Thread.create agent_connection_guts (client_sock, other_addr, agents_working_mutex, agents_working_ref))
			with
				x -> (
					print "croaked with \"%s\"; restarting\n" (Printexc.to_string x);
				)
			);
(*
			Mutex.lock agents_working_mutex;
			decr agents_working_ref;
			Mutex.unlock agents_working_mutex;
*)
		)

	done

;;
connect_and_do ();;

**********************************)
























(********
(* Set up the connection *)
let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0;;

Unix.setsockopt sock Unix.SO_REUSEADDR true;;

print " Connecting...\n";;
Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, port));;

print " Listening...\n";;
Unix.listen sock 10;;

(**************)
(* MAIN LOUPE *)
(**************)
while true do
	if debug_file then print "DOING!\n";

	let (client_sock, other_addr) = Unix.accept sock in

	try (

		(match other_addr with
			| Unix.ADDR_INET (x,y) -> (print " Got a connection from %s:%d\n" (Unix.string_of_inet_addr x) y)
			| _ -> (print " Got a Unix connection???\n"; failwith "Weird connection")
		);
(*		let (s_in, s_out) = (Unix.in_channel_of_descr client_sock, Unix.out_channel_of_descr client_sock) in*)
		let recv () = recv client_sock in
		let send = send client_sock in
(*		let unexpected_packet (s,t) = (Failure (Printf.sprintf "Tag %S (length %d) was not expected here" s (String.length t))) in*)

		(* Make sure the sender is sane *)
		(match recv () with
		| ("HELO", "") -> () (* Got a proper connection *)
		| x -> raise (unexpected_packet x)
		);


		(* Each pass needs at least pass options, zone options, and a range. 2nd pass also needs the 1st pass stats file *)
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

			print " 1st pass\n";

(*			let stats_file = Filename.concat temp_dir (Printf.sprintf "%d %d.txt" f_from f_to) in*)
			let stats_file = temp_file_name (Filename.concat temp_dir (Printf.sprintf "%d %d " f_from f_to)) ".txt" in
			let stats_temp_file = stats_file ^ ".temp" in

			(* Pass it to x264! *)
			let x264_string = if nice = 0 then (
				(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
				Printf.sprintf "%s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
			) else (
				Printf.sprintf "nice -n %d %s %s --fps %d/%d --pass 1 --stats \"%s\" %s -o %s - %dx%d" nice x264_exe opts fps_n fps_d stats_file zone_option dev_null res_x res_y
			) in
			print "Doing the following:\n  %s\n" x264_string;
			let put = Unix.open_process_out x264_string in
			(try
				let rec add_frame () = (
					match recv () with
					| ("FRAM",x)  -> (output_string put x; add_frame ())
					| ("EOF!","") -> ()
					| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
				) in
				add_frame ();
				flush put;
			with
			| x -> (
				(* Close the process if something bad happens *)
				ignore (Unix.close_process_out put);
				raise x
			));

			(match (Unix.close_process_out put) with
				| Unix.WEXITED x -> print "Exited %d\n" x
				| Unix.WSIGNALED x -> print "Signaled %d?\n" x
				| Unix.WSTOPPED x -> print "Stopped %d\n" x
			);

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

			print " 2nd pass\n";

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
					| ("EOF!","") -> (print "Done recieving stats\n")
					| x -> raise (unexpected_packet x)
				) in
				recv_stats ();
			with
				e -> (close_out temp_stats_handle; raise e)
			);
			close_out temp_stats_handle;

			let mkv_file = (
				let (file, handle) = open_temp_file (Filename.concat temp_dir "output ") ".mkv" in
				close_out handle;
				file
			) in

			(* Pass it to x264! *)
			let x264_string = if nice = 0 then (
				(* Don't bother with using nice, cuz it won't help. (Useful for people who don't have it) *)
				Printf.sprintf "%s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
			) else (
				Printf.sprintf "nice -n %d %s %s --bitrate %d --fps %d/%d --pass 3 --stats \"%s\" %s -o \"%s\" - %dx%d" nice x264_exe opts target_bitrate fps_n fps_d temp_stats_name zone_option mkv_file res_x res_y
			) in
			print "Doing the following:\n  %s\n" x264_string;
			let put = Unix.open_process_out x264_string in
			(try
				let rec add_frame () = (
					match recv () with
					| ("FRAM",x)  -> (output_string put x; add_frame ())
					| ("EOF!","") -> ()
					| x -> (ignore (Unix.close_process_out put); raise (unexpected_packet x))
				) in
				add_frame ();
				flush put;
			with
				(* This should fix some problems with too many open files... *)
				e -> (ignore (Unix.close_process_out put); raise e)
			);
			(match (Unix.close_process_out put) with
				| Unix.WEXITED x -> print "Exited %d\n" x
				| Unix.WSIGNALED x -> print "Signaled %d?\n" x
				| Unix.WSTOPPED x -> print "Stopped %d\n" x
			);
			print "DONE\n";

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
			Sys.remove temp_stats_name; (* Don't need that anymore *)

			(* Now output the MKV *)
			print "Sending MKV\n";
			let mkv_handle = open_in_bin mkv_file in
			(try
				let read_and_output = (
					let temp_string = String.create 4096 in
					let rec a () = (
						let actually_read = input mkv_handle temp_string 0 4096 in
(*						print "Grabbed %d bytes\n" actually_read;*)
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
			Sys.remove mkv_file;
		)
		| x -> raise (unexpected_packet x);
		); (* Big match statement *)
		print "Finished OK; shutting down socket\n";
		Unix.close client_sock;
	) with
	| Sys.Break -> (print "Got a break! exiting...\n"; raise Sys.Break)
	| Unix.Unix_error (a,b,c) -> (print "Agent croaked with exception \"Unix.Unix_error(%S,%S,%S)\"; restarting\n" (Unix.error_message a) b c; Unix.close client_sock)
	| x -> (print "Agent croaked with exception %S; restarting\n" (Printexc.to_string x); Unix.close client_sock)
done;;
********)
