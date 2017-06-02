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

open Pack;;

let version = "1.15-182";;

let to_hex s =
	let result = String.create (2 * String.length s) in
	for i = 0 to String.length s - 1 do
		String.blit (Printf.sprintf "%02X" (int_of_char s.[i])) 0 result (2*i) 2;
	done;
	result
;;

let to_bin =
	let lookup = [| 128;64;32;16;8;4;2;1 |] in
	fun s -> (
		let result = String.create (8 * String.length s) in
		for chr = 0 to String.length s - 1 do
			let code = Char.code s.[chr] in
			for bit = 0 to 7 do
				result.[(chr lsl 3) lor bit] <- if (code land lookup.(bit) = 0) then '0' else '1'
			done
		done;
		result
	)
;;

let string_xor a b = 
	let len = min (String.length a) (String.length b) in
	let out = String.create len in
	for i = 0 to len - 1 do
		out.[i] <- Char.chr (Char.code a.[i] lxor Char.code b.[i])
	done;
	out
;;

let partial_array_iter f a from len =
	if from < 0 || len < 0 || from + len > Array.length a then (
		raise (Invalid_argument "Types.partial_array_iter")
	) else (
		let rec r now left = (
			if left = 0 then () else (
				f a.(now);
				r (succ now) (pred left)
			)
		) in
		r from len
	)
;;

exception Transmission_error of string;;

exception Unsupported_argument of string;;
type bitrate_t = Percent of int | Kbps of float;;

type zone_type_t = Zone_bitrate of float | Zone_q of float;;
type zone_entry_t = {zone_start : int; zone_end : int; zone_type : zone_type_t};;
exception Invalid_zone of string;;

exception Invalid_config of string;;

exception Timeout;;

type 'a option3_t = Zero | One of 'a | Many of 'a option3_t array;;

type ac_t = {
	ac_name : string;
	ac_ip : string;
	ac_port : int;
	ac_agents : int * int;
};;

type option_t = {
	o_first : string;
	o_second : string;
	o_zones : zone_entry_t list;
	o_seek : int;
	o_frames : int;
	o_batch_length : int;
	o_final_batch_mult : float;
	o_split_frame_length : int;
	o_split_thresh : float;
	o_rethresh : float;
	o_output_file : string;
	o_input_avs : string;
	o_input_avs_full : string;
	o_input_avs_md5 : string;
	o_input_fast_avs : string;
	o_input_first_avs : string;
	o_input_first_avs_full : string;
	o_input_first_avs_md5 : string;
	o_second_pass_bitrate : bitrate_t;
	o_third_pass_threshold : float;
	o_third_pass_max_gops : int;
	o_third_pass_max_gop_ratio : float;
	o_ratecontrol_gops : int;
	o_size_precision : float;
	o_temp_dir : string;
	o_preseek : int;
	o_force : bool;
	o_restart : bool;
	o_savedisk : bool;
	o_keeptemp : bool;
	o_nocomp : bool;
(*	o_agents_1 : (string * (string * int * int) * int) array; (* ("name", ("ip", port_from, port_to), number) *)
	o_agents_2 : (string * (string * int * int) * int) array;*)
	o_agents : ac_t array;

	o_rc_qcomp : float;
	o_rc_cplxblur : float;
	o_rc_qblur : float;
	o_rc_qpmin : int;
	o_rc_qpmax : int;
	o_rc_qpstep : int;
	o_rc_ipratio : float;
	o_rc_pbratio : float;

	o_argv : string array;

	o_avs2yuv : string option; (* This is what the user selects for avs2yuv *)

	o_adhoc_enabled : bool;
	o_adhoc_controller : int; (* Listen port for controller *)
	o_adhoc_agent : int; (* Send port to agents *)
};;

type info_t = {
	i_res_x : int;
	i_res_y : int;
	i_fps_n : int;
	i_fps_d : int;
	i_fps_f : float;
	i_num_frames : int;
	i_bytes_y : int;
	i_bytes_uv : int;
	i_bytes_per_frame : int;

	i_get_fast_frame : int -> (string * string * string);
	i_get_fast_frame_y : int -> string;
	i_fast_res_x : int;
	i_fast_res_y : int;
	i_fast_frame_diff : string -> string -> int;

	i_avs2yuv : string; (* This is what the computer finds for avs2yuv *)
(*
	i_frame_diff : string -> string -> int;
	i_get_frame_with_string : string -> int -> unit;
	i_get_frame : int -> string;
	i_frame_put : out_channel;
	i_frame_get : in_channel;
*)
};;

let dev_null = if Sys.os_type = "Win32" then "NUL" else "/dev/null";;

let max_string_length = 16777211;; (* This is the max string length of 32-bit OCaml. When sending over the network, this is the safest maximum to use *)
let max_string_length = 16777200;; (* This is so that the entire string can be sent with the header *)
(*let max_string_length = 4096;;*)
(*let max_string_length = 8960 - 8;; (* The maximum TCP size with 9000 byte jumbo frames (minus 8 for my program's header) *)*)
let max_string_length = 1048576;;
let max_string_length = 524288;;

let max_string_length_bin = packN max_string_length;;

type file_t = File_DNE | File_file | File_dir;;
let is_dir x = try (
	match (Unix.LargeFile.stat x).Unix.LargeFile.st_kind with
	| Unix.S_DIR -> true
	| _ -> false
) with
	_ -> false
;;
let is_file x = try (
	match (Unix.LargeFile.stat x).Unix.LargeFile.st_kind with
	| Unix.S_REG -> true
	| _ -> false
) with
	_ -> false
;;
let is_something x = is_file x || is_dir x;;

let search_for_file x = (
	if Filename.is_implicit x then (
		(* Look in all the standard places *)
		if is_file (Filename.concat Filename.current_dir_name x) then (
			(* Found it in the CWD *)
			Some (Filename.concat Filename.current_dir_name x)
		) else if is_file (Filename.concat (Filename.dirname Sys.executable_name) x) then (
			(* Found it in the executable dir *)
			Some (Filename.concat (Filename.dirname Sys.executable_name) x)
		) else (
			None
		)
	) else (
		if is_file x then (
			Some x
		) else (
			None
		)
	)
);;

let split_last_dir x = (Filename.dirname x, Filename.basename x);;
let rec split_on_dirs_rev x = (
	let (dir, base) = split_last_dir x in
	if dir = x then (
		if base = "." then (
			[dir]
		) else (
			base :: [dir]
		)
	) else if base = "." then (
		split_on_dirs_rev dir
	) else (
		base :: (split_on_dirs_rev dir)
	)
);;
let split_on_dirs x = List.rev (split_on_dirs_rev x);;

type frame_t = Frame_I of string | Frame_notI of string | No_frame;;
(*
type frame_range_t = Range_found of int * int * int | Range_working | Range_none;; (* Used for the ranger. Range_working means that no ranges have been found, but a Frame_working has, and therefore the ranger may not be done *)
*)
(*
type range_t = Range_empty of (int * int) | Range_full of (int * int * frame_t array) | Range_queued of (int * int) | Range_working of ((int * int) * string * bool);;
*)

type range_type_t =
	| Range_empty
	| Range_full of frame_t array (* Stats for the range's frames *)
	| Range_queued
	| Range_working of (string) (* Worker name *)
;;
type range_endpoint_t = {
	mutable range_prev : range_t option;  (* The range before this endpoint *)
	mutable range_next : range_t option;  (* The range after this endpoint *)
	mutable range_delfirst : bool; (* If the point is invalid but can't be changed for some reason, set this *)
	range_frame  : int;             (* The first frame in the next range (if this is the last endpoint it should be set to i.i_num_frames) *)
	range_thresh : float;           (* The final split threshold used when finding this split point. Points which are created when other ranges are done should use infinity *)
} and range_t = {
	range_type  : range_type_t;     (* Type of the range... *)
	range_left  : range_endpoint_t; (* Endpoint which represents the start of this range *)
	range_right : range_endpoint_t; (* Endpoint which represents the start of the next frame (and one frame after the end of this range) *)
};;

(* Sticks a range of the given type between two frames *)
let add_range p1 range_type p2 =
	let r = {range_type = range_type; range_left = p1; range_right = p2} in
	p1.range_next <- Some r;
	p2.range_prev <- Some r;
;;
(* Might as well return the range, since it's difficult to get to otherwise *)
let add_range_and_return p1 range_type p2 =
	let r = {range_type = range_type; range_left = p1; range_right = p2} in
	p1.range_next <- Some r;
	p2.range_prev <- Some r;
	r
;;


(* This next variable approximates the number of extra bits added to the first frame in each encode. x264 adds the encoding options to the beginning, but that means every GOP will have this many bits added to it (which is completely unneccesary) *)
let first_frame_extra_bits = 587 * 8;;





(*failwith (Printf.sprintf "Use console functions? %B" use_console_functions);;*)


(********************************************************************************)
(* Emulates the Filename.open_temp_file function, but in an arbitrary directory *)
(* I guess this file is more like a handy function file than a types file...    *)
(********************************************************************************)
let prng = Random.State.make_self_init ();;

let temp_file_name prefix suffix =
  let rnd = (Random.State.bits prng) land 0xFFFFFF in
  Printf.sprintf "%s%06x%s" prefix rnd suffix
;;
(*
let temp_file prefix suffix =
  let rec try_name counter =
    if counter >= 1000 then
      invalid_arg "Types.temp_file: temp dir nonexistent or full";
    let name = temp_file_name prefix suffix in
    try
      close_desc(open_desc name [Open_wronly; Open_creat; Open_excl] 0o600);
      name
    with Sys_error _ ->
      try_name (counter + 1)
  in try_name 0
;;
*)
let open_temp_file ?(mode = [Open_text]) prefix suffix =
  let rec try_name counter =
    if counter >= 1000 then
      invalid_arg "Types.open_temp_file: temp dir nonexistent or full";
    let name = temp_file_name prefix suffix in
    try
      (name,
       open_out_gen (Open_wronly::Open_creat::Open_excl::mode) 0o600 name)
    with Sys_error _ ->
      try_name (counter + 1)
  in try_name 0
;;

(*********************)
(* SECOND PASS TYPES *)
(*********************)
type stats_frame_t = Stats_frame_I | Stats_frame_P | Stats_frame_B | Stats_frame_i | Stats_frame_b;;
type stats_g_t = Stats_general_I | Stats_general_P | Stats_general_B;;
let generalize_frame_types = function
	| Stats_frame_I -> Stats_general_I
	| Stats_frame_P -> Stats_general_P
	| Stats_frame_B -> Stats_general_B
	| Stats_frame_i -> Stats_general_I
	| Stats_frame_b -> Stats_general_B
;;
let char_of_stats_frame = function
	| Stats_frame_I -> 'I'
	| Stats_frame_P -> 'P'
	| Stats_frame_B -> 'B'
	| Stats_frame_i -> 'i'
	| Stats_frame_b -> 'b'
;;

type stats_line = {
	mutable stats_in : int;
	mutable stats_out : int;
	mutable stats_type : stats_frame_t;
	mutable stats_q : float;
	mutable stats_qscale : float;
	mutable stats_itex : int;
	mutable stats_ptex : int;
	mutable stats_mv : int;
	mutable stats_misc : int;
	mutable stats_imb : int;
	mutable stats_pmb : int;
	mutable stats_smb : int;
	mutable stats_d : char;
	mutable stats_comp : float;
	mutable stats_blurcomp : float;
	mutable stats_out_qscale : float;
	mutable stats_out_blurqscale : float;
	mutable stats_zone : zone_type_t;
};;
let null_stats_line = {
	stats_in = -1;
	stats_out = -1;
	stats_type = Stats_frame_b;
	stats_q = 0.0;
	stats_qscale = 0.0;
	stats_itex = 0;
	stats_ptex = 0;
	stats_mv = 0;
	stats_misc = 0;
	stats_imb = 0;
	stats_pmb = 0;
	stats_smb = 0;
	stats_d = '-';
	stats_comp = 0.0;
	stats_blurcomp = 0.0;
	stats_out_qscale = 0.0;
	stats_out_blurqscale = 0.0;
	stats_zone = Zone_bitrate 1.0;
};;


type transit_gop_t = {
	mutable gop_number : int;
	mutable gop_frame_offset : int;
	mutable gop_optimal_bits : float; (* The number of bits from the ratecontrol equation *)
	mutable gop_attempted_bits : (float * float) option; (* A previous pass attempt's (target,actual) bits. *)
	mutable gop_times_completed : int; (* The number of times that the GOP was finished successfully *)
	mutable gop_frames : stats_line array;
	mutable gop_display_frames : stats_line array;
	mutable gop_zones : string;
	mutable gop_output_file_name : string;
	mutable gop_initial_min_bitrate : int;
	mutable gop_current_min_bitrate : int;
};;
let null_gop = {
	gop_number = -1;
	gop_frame_offset = -1;
	gop_optimal_bits = 0.0;
	gop_attempted_bits = None;
	gop_times_completed = 0;
	gop_frames = [||];
	gop_display_frames = [||];
	gop_zones = "";
	gop_output_file_name = "";
	gop_initial_min_bitrate = 1;
	gop_current_min_bitrate = 1;
};;

type info2_t = {
	i2_name : string;
	mutable i2_total_gops : int;
	mutable i2_total_frames : int;
	mutable i2_current_gop : int option;
	mutable i2_current_checkpoint : float option; (* Checkpoint for FPS meter *)
	mutable i2_current_frames : int; (* The number of frames between the last and current checkpoint *)
	mutable i2_current_fps : float;
};;

(* qp_i is the H.264 quantizer, qp_f is the float version of it, and qscale is the linearized quantizer *)
let qscale_of_qp_i qp = (
	0.85 *. 2.0 ** ((float_of_int (qp - 12)) /. 6.0)
);;
let qscale_of_qp_f qp = (
	0.85 *. 2.0 ** ((qp -. 12.0) /. 6.0)
);;
let qp_f_of_qscale qs = (
	12.0 +. 6.0 *. log (qs /. 0.85) /. log 2.0
);;
let qp_i_of_qscale qs = (
	int_of_float (qp_f_of_qscale qs +. 0.5)
);;
let bits_of_qscale rce newQ =
	let newQ = max newQ 0.1 in
	(float_of_int rce.stats_itex +. float_of_int rce.stats_ptex +. 0.1) *. (rce.stats_qscale /. newQ) ** 1.1
		+. float_of_int rce.stats_mv *. ((max 1.0 rce.stats_qscale) /. (max 1.0 newQ)) ** 0.5
		+. float_of_int rce.stats_misc
;;

let string_of_stats_line ?(add_bits=0) s = (
	Printf.sprintf "in:%d out:%d type:%c q:%.2f itex:%d ptex:%d mv:%d misc:%d imb:%d pmb:%d smb:%d d:%c;" s.stats_in s.stats_out (char_of_stats_frame s.stats_type) s.stats_q s.stats_itex s.stats_ptex s.stats_mv (s.stats_misc + add_bits) s.stats_imb s.stats_pmb s.stats_smb s.stats_d
);;
let stats_line_of_string = (
	let line_rx = Str.regexp ".*in:\\([0-9]+\\) out:\\([0-9]+\\) type:\\([IPBib]\\) q:\\([0-9.]+\\) itex:\\([0-9]+\\) ptex:\\([0-9]+\\) mv:\\([0-9]+\\) misc:\\([0-9]+\\) imb:\\([0-9]+\\) pmb:\\([0-9]+\\) smb:\\([0-9]+\\) d:\\(.\\)" in
	fun line -> (
		let stats_ok = Str.string_match line_rx line 0 in
		if stats_ok then (
			let stats = {
				stats_in   = int_of_string (Str.matched_group  1 line);
				stats_out  = int_of_string (Str.matched_group  2 line);
				stats_type = (match Str.matched_group 3 line with "I" -> Stats_frame_I | "P" -> Stats_frame_P | "B" -> Stats_frame_B | "i" -> Stats_frame_i | "b" -> Stats_frame_b | x -> failwith (Printf.sprintf "Don't know what frame type %S means in frame %S\n" x line));
				stats_q    = float_of_string (Str.matched_group 4 line);
				stats_qscale = qscale_of_qp_f (float_of_string (Str.matched_group 4 line));
				stats_itex = int_of_string (Str.matched_group  5 line);
				stats_ptex = int_of_string (Str.matched_group  6 line);
				stats_mv   = int_of_string (Str.matched_group  7 line);
				stats_misc = int_of_string (Str.matched_group  8 line);
				stats_imb  = int_of_string (Str.matched_group  9 line);
				stats_pmb  = int_of_string (Str.matched_group 10 line);
				stats_smb  = int_of_string (Str.matched_group 11 line);
				stats_d    = (Str.matched_group 12 line).[0];
				stats_comp = 1.0;
				stats_blurcomp = 1.0;
				stats_out_qscale = 1.0;
				stats_out_blurqscale = 1.0;
				stats_zone = Zone_bitrate 1.0;
			} in
			let comp = bits_of_qscale stats 1.0 -. float_of_int stats.stats_misc in
			stats.stats_comp <- comp;
			stats.stats_blurcomp <- comp;
			Some stats;
		) else (
			None
		)
	)
);;




let unix_error_string = function
	| Unix.E2BIG           -> "2BIG"
	| Unix.EACCES          -> "ACCES"
	| Unix.EAGAIN          -> "AGAIN"
	| Unix.EBADF           -> "BADF"
	| Unix.EBUSY           -> "BUSY"
	| Unix.ECHILD          -> "CHILD"
	| Unix.EDEADLK         -> "DEADLK"
	| Unix.EDOM            -> "DOM"
	| Unix.EEXIST          -> "EXIST"
	| Unix.EFAULT          -> "FAULT"
	| Unix.EFBIG           -> "FBIG"
	| Unix.EINTR           -> "INTR"
	| Unix.EINVAL          -> "INVAL"
	| Unix.EIO             -> "IO"
	| Unix.EISDIR          -> "ISDIR"
	| Unix.EMFILE          -> "MFILE"
	| Unix.EMLINK          -> "MLINK"
	| Unix.ENAMETOOLONG    -> "NAMETOOLONG"
	| Unix.ENFILE          -> "NFILE"
	| Unix.ENODEV          -> "NODEV"
	| Unix.ENOENT          -> "NOENT"
	| Unix.ENOEXEC         -> "NOEXEC"
	| Unix.ENOLCK          -> "NOLCK"
	| Unix.ENOMEM          -> "NOMEM"
	| Unix.ENOSPC          -> "NOSPC"
	| Unix.ENOSYS          -> "NOSYS"
	| Unix.ENOTDIR         -> "NOTDIR"
	| Unix.ENOTEMPTY       -> "NOTEMPTY"
	| Unix.ENOTTY          -> "NOTTY"
	| Unix.ENXIO           -> "NXIO"
	| Unix.EPERM           -> "PERM"
	| Unix.EPIPE           -> "PIPE"
	| Unix.ERANGE          -> "RANGE"
	| Unix.EROFS           -> "ROFS"
	| Unix.ESPIPE          -> "SPIPE"
	| Unix.ESRCH           -> "SRCH"
	| Unix.EXDEV           -> "XDEV"
	| Unix.EWOULDBLOCK     -> "WOULDBLOCK"
	| Unix.EINPROGRESS     -> "INPROGRESS"
	| Unix.EALREADY        -> "ALREADY"
	| Unix.ENOTSOCK        -> "NOTSOCK"
	| Unix.EDESTADDRREQ    -> "DESTADDRREQ"
	| Unix.EMSGSIZE        -> "MSGSIZE"
	| Unix.EPROTOTYPE      -> "PROTOTYPE"
	| Unix.ENOPROTOOPT     -> "NOPROTOOPT"
	| Unix.EPROTONOSUPPORT -> "PROTONOSUPPORT"
	| Unix.ESOCKTNOSUPPORT -> "SOCKTNOSUPPORT"
	| Unix.EOPNOTSUPP      -> "OPNOTSUPP"
	| Unix.EPFNOSUPPORT    -> "PFNOSUPPORT"
	| Unix.EAFNOSUPPORT    -> "AFNOSUPPORT"
	| Unix.EADDRINUSE      -> "ADDRINUSE"
	| Unix.EADDRNOTAVAIL   -> "ADDRNOTAVAIL"
	| Unix.ENETDOWN        -> "NETDOWN"
	| Unix.ENETUNREACH     -> "NETUNREACH"
	| Unix.ENETRESET       -> "NETRESET"
	| Unix.ECONNABORTED    -> "CONNABORTED"
	| Unix.ECONNRESET      -> "CONNRESET"
	| Unix.ENOBUFS         -> "NOBUFS"
	| Unix.EISCONN         -> "ISCONN"
	| Unix.ENOTCONN        -> "NOTCONN"
	| Unix.ESHUTDOWN       -> "SHUTDOWN"
	| Unix.ETOOMANYREFS    -> "TOOMANYREFS"
	| Unix.ETIMEDOUT       -> "TIMEDOUT"
	| Unix.ECONNREFUSED    -> "CONNREFUSED"
	| Unix.EHOSTDOWN       -> "HOSTDOWN"
	| Unix.EHOSTUNREACH    -> "HOSTUNREACH"
	| Unix.ELOOP           -> "LOOP"
	| Unix.EOVERFLOW       -> "OVERFLOW"
	| Unix.EUNKNOWNERR x   -> ("UNKNOWN " ^ string_of_int x)
;;
