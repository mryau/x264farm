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

exception Invalid_integer;;
exception Invalid_integer_length of int64;;
exception Invalid_float_length of int64;;
exception Unsupported_float;;
exception Invalid_ID of int64;;
exception Unknown_ID of (int * int64);;
exception ID_string_too_short of (int * int);;
exception Invalid_element_position of string;;
exception String_too_large of (int64 * int64);;
exception Loop_end;;
exception Matroska_error of string;;

(* Int64 functions (I don't want to keep typing garbage) *)
let ( +|  ) = Int64.add;;
let ( -|  ) = Int64.sub;;
let ( *|  ) = Int64.mul;;
let ( /|  ) = Int64.div;;
let ( <|  ) = Int64.shift_left;;
let ( >|  ) = Int64.shift_right_logical;;
let ( >|- ) = Int64.shift_right;;
let ( ||| ) = Int64.logor;;
let ( &&| ) = Int64.logand;;
let ( !|  ) = Int64.of_int;;
let ( !-  ) = Int64.to_int;;

(*********************)
(* Reading functions *)
(*********************)
let rec read_uint ?(so_far=0L) f len =
	if len > 8L || len < 0L
		then raise (Invalid_integer_length len)
		else if len = 0L
			then so_far
			else (
				let b = input_byte f in
				read_uint ~so_far:((so_far <| 8) ||| ( !| b )) f (len -| 1L)
			);;

let read_sint f len =
	if len > 8L || len < 0L
		then raise (Invalid_integer_length len)
		else (
			if len = 0L
				then 0L
				else (
					(* This line extends the sign from the first byte read to all the bytes in the int64 *)
					let b = (!| (input_byte f)) <| 56 >|- 56 in
					read_uint ~so_far:b f (len -| 1L)
				)
		);;

let rec read_xiph ?(so_far=0L) f =
	let b = !| (input_byte f) in
	if b = 255L
		then read_xiph ~so_far:(so_far +| 255L) f
		else so_far +| b;;

let read_len f =
	let b1 = !| (input_byte f) in
	let (noot, mask) = if b1 &&| 0x80L <> 0L then (
		(* 1 byte *)
		((b1 &&| 0x7FL), 0x7FL)
	) else if b1 &&| 0x40L <> 0L then (
		(* 2 bytes *)
		(read_uint ~so_far:(b1 &&| 0x3FL) f 1L, 0x3FFFL)
	) else if b1 &&| 0x20L <> 0L then (
		(* 3 bytes *)
		(read_uint ~so_far:(b1 &&| 0x1FL) f 2L, 0x1FFFFFL)
	) else if b1 &&| 0x10L <> 0L then (
		(* 4 bytes *)
		(read_uint ~so_far:(b1 &&| 0x0FL) f 3L, 0x0FFFFFFFL)
	) else if b1 &&| 0x08L <> 0L then (
		(* 5 bytes *)
		(read_uint ~so_far:(b1 &&| 0x07L) f 4L, 0x07FFFFFFFFL)
	) else if b1 &&| 0x04L <> 0L then (
		(* 6 bytes *)
		(read_uint ~so_far:(b1 &&| 0x03L) f 5L, 0x03FFFFFFFFFFL)
	) else if b1 &&| 0x02L <> 0L then (
		(* 7 bytes *)
		(read_uint ~so_far:(b1 &&| 0x01L) f 6L, 0x01FFFFFFFFFFFFL)
	) else if b1 &&| 0x01L <> 0L then (
		(* 8 bytes *)
		(read_uint                        f 7L, 0x00FFFFFFFFFFFFFFL)
	) else (
		(* Oops *)
		raise Invalid_integer
	) in
	if noot = mask then None else Some noot;;

let read_float f len =
	if len = 4L then (
		let f64 = read_sint f 4L in
		Int32.float_of_bits (Int64.to_int32 f64)
	) else if len = 8L then (
		Int64.float_of_bits (read_sint f 8L)
	) else (
		raise (Invalid_float_length len)
	);;

let read_id f =
	let b1 = !| (input_byte f) in
	if b1 &&| 0x80L <> 0L then (
		(* 1 byte *)
		(b1 &&| 0x7FL)
	) else if b1 &&| 0x40L <> 0L then (
		(* 2 bytes *)
		read_uint ~so_far:(b1 &&| 0x3FL) f 1L
	) else if b1 &&| 0x20L <> 0L then (
		(* 3 bytes *)
		read_uint ~so_far:(b1 &&| 0x1FL) f 2L
	) else if b1 &&| 0x10L <> 0L then (
		(* 4 bytes *)
		read_uint ~so_far:(b1 &&| 0x0FL) f 3L
	) else (
		raise (Invalid_ID (LargeFile.pos_in f))
	);;

(********************)
(* Read from string *)
(********************)
let rec read_uint_from_string ?(so_far=0L) str off len =
	(* len is an int64, whereas off is an int... It just fits better that way *)
	if len > 8L || len < 0L
		then raise (Invalid_integer_length len)
		else if len = 0L
			then so_far
			else (
				read_uint_from_string ~so_far:((so_far <| 8) ||| ( !| (Char.code str.[off]))) str (off + 1) (len -| 1L)
			);;

let read_sint_from_string str off len =
	if len > 8L || len < 0L
		then raise (Invalid_integer_length len)
		else (
			if len = 0L
				then 0L
				else (
					let b = (!| (Char.code str.[off])) <| 56 >|- 56 in
					read_uint_from_string ~so_far:b str (off + 1) (len -| 1L)
				)
		);;

(* Returns (value, number_of_bytes_read) *)
let rec read_xiph_from_string ?(so_far=0L) ?(total_bytes=0L) str off =
	let b = !| (Char.code str.[off]) in
	if b = 255L
		then read_xiph_from_string ~so_far:(so_far +| 255L) ~total_bytes:(total_bytes +| 1L) str (succ off) (* suck off! *)
		else (so_far +| b, total_bytes +| 1L);;

(* Also returns (Some value, number_of_bytes_read) *)
(* OR (None, number_of_bytes_read) if the length is undefined *)
let read_len_from_string str off =
	let b1 = !| (Char.code str.[off]) in
	let (noot, mask, bytes) = if b1 &&| 0x80L <> 0L then (
		(* 1 byte *)
		((b1 &&| 0x7FL), 0x7FL, 1L)
	) else if b1 &&| 0x40L <> 0L then (
		(* 2 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x3FL) str (succ off) 1L, 0x3FFFL, 2L)
	) else if b1 &&| 0x20L <> 0L then (
		(* 3 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x1FL) str (succ off) 2L, 0x1FFFFFL, 3L)
	) else if b1 &&| 0x10L <> 0L then (
		(* 4 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x0FL) str (succ off) 3L, 0x0FFFFFFFL, 4L)
	) else if b1 &&| 0x08L <> 0L then (
		(* 5 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x07L) str (succ off) 4L, 0x07FFFFFFFFL, 5L)
	) else if b1 &&| 0x04L <> 0L then (
		(* 6 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x03L) str (succ off) 5L, 0x03FFFFFFFFFFL, 6L)
	) else if b1 &&| 0x02L <> 0L then (
		(* 7 bytes *)
		(read_uint_from_string ~so_far:(b1 &&| 0x01L) str (succ off) 6L, 0x01FFFFFFFFFFFFL, 7L)
	) else if b1 &&| 0x01L <> 0L then (
		(* 8 bytes *)
		(read_uint_from_string                        str (succ off) 7L, 0x00FFFFFFFFFFFFFFL, 8L)
	) else (
		(* Oops *)
		raise Invalid_integer
	) in
	if noot = mask then (None, bytes) else (Some noot, bytes)
;;

type lacing_t = Lacing_none | Lacing_xiph | Lacing_ebml | Lacing_fixed;;
type block_t = {
	mutable block_track_number : int64;
	mutable block_timecode : int;
	mutable block_invisible : bool;
	mutable block_lacing : lacing_t;
	mutable block_frames : string list;
}
let block_of_string str =
	let (track_num_opt, track_num_len) = read_len_from_string str 0 in
	let track_num = match (track_num_opt, track_num_len) with
		| (Some x, _) -> x
		| (None, 1L) -> 127L
		| (None, 2L) -> 16383L
		| (None, 3L) -> 2097151L
		| (None, 4L) -> 268435455L
		| (None, 5L) -> 34359738367L
		| (None, 6L) -> 4398046511103L
		| (None, 7L) -> 562949953421311L
		| (None, _ ) -> 72057594037927935L
	in
	let timecode = !- (read_sint_from_string str (!- track_num_len) 2L) in (* The timecode is always 2 bytes, so it's OK to turn it into an int *)
	let flag_byte = Char.code str.[!- track_num_len + 2] in
	let invisible = flag_byte land 0x10 <> 0 in
	let frame_start_off = !- track_num_len + 3 in
	let total_frame_length = String.length str - frame_start_off in
	let (lacing, frames) = match flag_byte land 0x60 with
		| 0x00 -> (Lacing_none, [String.sub str frame_start_off total_frame_length])
		| 0x20 -> (Lacing_xiph, [])
		| 0x40 -> (Lacing_ebml, [])
		|  _   -> (Lacing_fixed, [])
	in
	{
		block_track_number = track_num;
		block_timecode = timecode;
		block_invisible = invisible;
		block_lacing = lacing;
		block_frames = frames;
	}
;;

(*********************)
(* Testing functions *)
(*********************)
let len_length len =
	if len < 127L then
		1L
	else if len < 16383L then
		2L
	else if len < 2097151L then
		3L
	else if len < 268435455L then
		4L
	else if len < 34359738367L then
		5L
	else if len < 4398046511103L then
		6L
	else if len < 562949953421311L then
		7L
	else if len < 72057594037927935L then
		8L
	else
		raise (Invalid_integer_length len)
;;

let id_length id =
	if id < 128L then
		1L
	else if id < 16384L then
		2L
	else if id < 2097152L then
		3L
	else if id < 268435456L then
		4L
	else if id < 34359738368L then
		5L
	else if id < 4398046511104L then
		6L
	else if id < 562949953421312L then
		7L
	else if id < 72057594037927936L then
		8L
	else
		raise (Invalid_integer_length id)
;;

let len_uint uint =
	if uint < 256L then
		1L
	else if uint < 65536L then
		2L
	else if uint < 16777216L then
		3L
	else if uint < 4294967296L then
		4L
	else if uint < 1099511627776L then
		5L
	else if uint < 281474976710656L then
		6L
	else if uint < 72057594037927936L then
		7L
	else
		8L
;;

let len_sint sint =
	if sint < 128L && sint >= -128L then
		1L
	else if sint < 32768L && sint >= -32768L then
		2L
	else if sint < 8388608L && sint >= -8388608L then
		3L
	else if sint < 2147483648L && sint >= -2147483648L then
		4L
	else if sint < 549755813888L && sint >= -549755813888L then
		5L
	else if sint < 140737488355328L && sint >= -140737488355328L then
		6L
	else if sint < 36028797018963968L && sint >= -36028797018963968L then
		7L
	else
		8L
;;



(*********************)
(* Writing functions *)
(*********************)
let rec write_uint_to_string n =
	if n < 0L then
		raise (Matroska_error (Printf.sprintf "A signed integer %Ld was passed to write_uint_to_string" n))
	else if n < 256L then
		String.make 1 (Char.chr (!- n))
	else
		(write_uint_to_string (n >| 8)) ^ (String.make 1 (Char.chr (!- (n &&| 255L)))) (* Logical shift! *)
;;

let write_uint f n = output_string f (write_uint_to_string n);;

let rec write_sint_to_string n =
	if n < 128L && n >= 0L then
		String.make 1 (Char.chr (!- n))
	else if n < 0L && n >= -128L then
		String.make 1 (Char.chr (!- n + 256))
	else
		(write_sint_to_string (n >|- 8)) ^ (String.make 1 (Char.chr (!- (n &&| 255L)))) (* Arithmetic shift! *)
;;

let write_sint f n = output_string f (write_sint_to_string n);;

let write_id_to_string n =
	match id_length n with
	| 1L -> write_uint_to_string (0x80L ||| n)
	| 2L -> write_uint_to_string (0x4000L ||| n)
	| 3L -> write_uint_to_string (0x200000L ||| n)
	| 4L -> write_uint_to_string (0x10000000L ||| n)
	| 5L -> write_uint_to_string (0x0800000000L ||| n)
	| 6L -> write_uint_to_string (0x040000000000L ||| n)
	| 7L -> write_uint_to_string (0x02000000000000L ||| n)
	| _  -> write_uint_to_string (0x0100000000000000L ||| n);;

let write_length_to_string n =
	if n < 0L then (
		(* If the length is less than 0 (impossible), then write an unknown length *)
		"\xFF"
	) else (
		match len_length n with
		| 1L -> write_uint_to_string (0x80L ||| n)
		| 2L -> write_uint_to_string (0x4000L ||| n)
		| 3L -> write_uint_to_string (0x200000L ||| n)
		| 4L -> write_uint_to_string (0x10000000L ||| n)
		| 5L -> write_uint_to_string (0x0800000000L ||| n)
		| 6L -> write_uint_to_string (0x040000000000L ||| n)
		| 7L -> write_uint_to_string (0x02000000000000L ||| n)
		| _  -> write_uint_to_string (0x0100000000000000L ||| n)
	);;

let write_length f n =
	if n < 0L then (
		(* Unknown *)
		output_string f "\xFF"
	) else (
		match len_length n with
		| 1L -> write_uint f (0x80L ||| n)
		| 2L -> write_uint f (0x4000L ||| n)
		| 3L -> write_uint f (0x200000L ||| n)
		| 4L -> write_uint f (0x10000000L ||| n)
		| 5L -> write_uint f (0x0800000000L ||| n)
		| 6L -> write_uint f (0x040000000000L ||| n)
		| 7L -> write_uint f (0x02000000000000L ||| n)
		| _  -> write_uint f (0x0100000000000000L ||| n)
	);;

let rec write_xiph f n =
	if n < 0L then (
		raise (Matroska_error (Printf.sprintf "A signed integer %Ld was passed to write_xiph" n))
	) else if n < 255L then (
		output_char f (Char.chr (!- n))
	) else (
		output_char f '\xFF';
		write_xiph f (n -| 255L)
	);;

let write_float_to_string out len =
	if len <= 4L then (
		let str = String.create 4 in
		let write_this = Int64.of_int32 (Int32.bits_of_float out) in
		str.[0] <- (Char.chr ( !- (write_this >| 24) land 255));
		str.[1] <- (Char.chr ( !- (write_this >| 16) land 255));
		str.[2] <- (Char.chr ( !- (write_this >|  8) land 255));
		str.[3] <- (Char.chr ( !-  write_this        land 255));
		str
	) else (
		let str = String.create 8 in
		let write_this = Int64.bits_of_float out in
		str.[0] <- (Char.chr ( !- (write_this >| 56) land 255));
		str.[1] <- (Char.chr ( !- (write_this >| 48) land 255));
		str.[2] <- (Char.chr ( !- (write_this >| 40) land 255));
		str.[3] <- (Char.chr ( !- (write_this >| 32) land 255));
		str.[4] <- (Char.chr ( !- (write_this >| 24) land 255));
		str.[5] <- (Char.chr ( !- (write_this >| 16) land 255));
		str.[6] <- (Char.chr ( !- (write_this >|  8) land 255));
		str.[7] <- (Char.chr ( !-  write_this        land 255));
		str
	);;

let write_float f out len =
	if len <= 4 then (
		let write_this = Int64.of_int32 (Int32.bits_of_float out) in
		output_char f (Char.chr ( !- (write_this >| 24) land 255));
		output_char f (Char.chr ( !- (write_this >| 16) land 255));
		output_char f (Char.chr ( !- (write_this >|  8) land 255));
		output_char f (Char.chr ( !-  write_this        land 255));
	) else (
		let write_this = Int64.bits_of_float out in
		output_char f (Char.chr ( !- (write_this >| 56) land 255));
		output_char f (Char.chr ( !- (write_this >| 48) land 255));
		output_char f (Char.chr ( !- (write_this >| 40) land 255));
		output_char f (Char.chr ( !- (write_this >| 32) land 255));
		output_char f (Char.chr ( !- (write_this >| 24) land 255));
		output_char f (Char.chr ( !- (write_this >| 16) land 255));
		output_char f (Char.chr ( !- (write_this >|  8) land 255));
		output_char f (Char.chr ( !-  write_this        land 255));
	);;

let string_of_block block =
	let track_num_string = match block.block_track_number with
		| x when x <= 128L               -> write_uint_to_string (0x80L ||| block.block_track_number)
		| x when x <= 32768L             -> write_uint_to_string (0x4000L ||| block.block_track_number)
		| x when x <= 8388608L           -> write_uint_to_string (0x200000L ||| block.block_track_number)
		| x when x <= 2147483648L        -> write_uint_to_string (0x10000000L ||| block.block_track_number)
		| x when x <= 549755813888L      -> write_uint_to_string (0x0800000000L ||| block.block_track_number)
		| x when x <= 140737488355328L   -> write_uint_to_string (0x040000000000L ||| block.block_track_number)
		| x when x <= 36028797018963968L -> write_uint_to_string (0x02000000000000L ||| block.block_track_number)
		| x                              -> write_uint_to_string (0x0100000000000000L ||| block.block_track_number)
	in
	let timecode_string = String.create 2 in
	timecode_string.[0] <- Char.chr (block.block_timecode lsr 8 land 255);
	timecode_string.[1] <- Char.chr (block.block_timecode land 255);
	let flag_code = (match block.block_lacing with
		| Lacing_none  -> 0x00
		| Lacing_xiph  -> 0x20
		| Lacing_ebml  -> 0x40
		| Lacing_fixed -> 0x60
	) lor (
		if block.block_invisible then 0x10 else 0x00
	) in
	let flag_string = String.make 1 (Char.chr (flag_code land 255)) in
	let frames_string = match (block.block_lacing, block.block_frames) with
		| (Lacing_none , [f1]) -> f1
		| (Lacing_none , x)    -> (failwith "string_of_block can't make a string out of multiple strings")
		| (Lacing_xiph , frames) -> ""
		| (Lacing_ebml , frames) -> ""
		| (Lacing_fixed, frames) -> ""
	in
	track_num_string ^ timecode_string ^ flag_string ^ frames_string;;

type
	elt_t = {elt_id : id_t; mutable elt_len : int64; mutable elt_innards : innards_t} and
	innards_t = In_sint of int64 | In_uint of int64 | In_float of float | In_string of string | In_utf8 of string | In_date of int64 | In_elt of elt_t list | In_binary of string list | In_unparsed of string list | In_skipped | In_unknown and
	id_t = {id_name : string; id_id : int64; } and

	(* Describe an element *)
(*	innards_t_t = T_uint | T_sint | T_float | T_string | T_utf8 | T_date | T_elt | T_binary | T_unparsed | T_skipped and*)
(*
	innards_t_t =
	| T_uint of (int64 -> int64) option
	| T_sint of (int64 -> int64) option
	| T_float of (float -> float) option
	| T_string of (string -> string) option
	| T_utf8 of (string -> string) option
	| T_date of (int64 -> int64) option
	| T_elt of (elt_t list -> elt_t list) option
	| T_binary of (string list -> string list) option
	| T_unparsed of (string list -> string list) option
	| T_skipped of (unit -> unit) option
	and
	elt_t_t = {t_name : string; t_id : int64; t_innards : innards_t_t}
	and
*)
	innards_t_t =
	| T_uint_f of (id_t * int64 * int64 -> elt_t list)
	| T_sint_f of (id_t * int64 * int64 -> elt_t list)
	| T_float_f of (id_t * int64 * float -> elt_t list)
	| T_string_f of (id_t * int64 * string -> elt_t list)
	| T_utf8_f of (id_t * int64 * string -> elt_t list)
	| T_date_f of (id_t * int64 * int64 -> elt_t list)
	| T_elt_f of (id_t * int64 * elt_t list -> elt_t list)
	| T_binary_f of (id_t * int64 * string list -> elt_t list)
	| T_unparsed_f of (id_t * int64 * string list -> elt_t list)
	| T_skipped_f of (id_t * int64 * unit -> elt_t list)
	| T_uint
	| T_sint
	| T_float
	| T_string
	| T_utf8
	| T_date
	| T_elt
	| T_binary
	| T_unparsed
	| T_skipped
	and
	elt_t_t = {t_name	: string; t_id : int64; t_innards : innards_t_t}
;;

let elt_unknown = {t_name = "UNKNOWN ELEMENT"; t_id = Int64.minus_one; t_innards = T_skipped};;
  
let ebml_dtd = [
	{t_name = "EBML"                      ; t_id = 0x0A45DFA3L; t_innards = T_elt   };
	{t_name = "EBMLVersion"               ; t_id = 0x0286L    ; t_innards = T_uint  };
	{t_name = "EBMLReadVersion"           ; t_id = 0x02F7L    ; t_innards = T_uint  };
	{t_name = "EBMLMaxIDLength"           ; t_id = 0x02F2L    ; t_innards = T_uint  };
	{t_name = "EBMLMaxSizeLength"         ; t_id = 0x02F3L    ; t_innards = T_uint  };
	{t_name = "DocType"                   ; t_id = 0x0282L    ; t_innards = T_string};
	{t_name = "DocTypeVersion"            ; t_id = 0x0287L    ; t_innards = T_uint  };
	{t_name = "DocTypeReadVersion"        ; t_id = 0x0285L    ; t_innards = T_uint  };
(*
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
*)
];;

let matroska_dtd = [
	{t_name = "CRC-32"                    ; t_id = 0x3FL      ; t_innards = T_binary};
	{t_name = "Void"                      ; t_id = 0x6CL      ; t_innards = T_binary};
	{t_name = "Segment"                   ; t_id = 0x08538067L; t_innards = T_elt   };
	{t_name = "SeekHead"                  ; t_id = 0x014D9B74L; t_innards = T_elt   };
	{t_name = "Seek"                      ; t_id = 0x0DBBL    ; t_innards = T_elt   };
	{t_name = "SeekID"                    ; t_id = 0x13ABL    ; t_innards = T_binary};
	{t_name = "SeekPosition"              ; t_id = 0x13ACL    ; t_innards = T_uint  };
	{t_name = "Info"                      ; t_id = 0x0549A966L; t_innards = T_elt   };
	{t_name = "SegmentUID"                ; t_id = 0x33A4L    ; t_innards = T_binary};
	{t_name = "SegmentFilename"           ; t_id = 0x3384L    ; t_innards = T_utf8  };
	{t_name = "PrevUID"                   ; t_id = 0x1CB923L  ; t_innards = T_binary};
	{t_name = "PrevFilename"              ; t_id = 0x1C83ABL  ; t_innards = T_utf8  };
	{t_name = "NextUID"                   ; t_id = 0x1EB923L  ; t_innards = T_binary};
	{t_name = "NextFilename"              ; t_id = 0x1E83BBL  ; t_innards = T_utf8  };
	{t_name = "SegmentFamily"             ; t_id = 0x0444L    ; t_innards = T_binary};
	{t_name = "ChapterTranslate"          ; t_id = 0x2924L    ; t_innards = T_elt   };
	{t_name = "ChapterTranslateEditionUID"; t_id = 0x29FCL    ; t_innards = T_uint  };
	{t_name = "ChapterTranslateCodec"     ; t_id = 0x29BFL    ; t_innards = T_uint  };
	{t_name = "ChapterTranslateID"        ; t_id = 0x29A5L    ; t_innards = T_binary};
	{t_name = "TimecodeScale"             ; t_id = 0x0AD7B1L  ; t_innards = T_uint  };
	{t_name = "SegmentDuration"           ; t_id = 0x0489L    ; t_innards = T_float };
	{t_name = "DateUTC"                   ; t_id = 0x0461L    ; t_innards = T_date  };
	{t_name = "Title"                     ; t_id = 0x3BA9L    ; t_innards = T_utf8  };
	{t_name = "MuxingApp"                 ; t_id = 0x0D80L    ; t_innards = T_utf8  };
	{t_name = "WritingApp"                ; t_id = 0x1741L    ; t_innards = T_utf8  };
	{t_name = "Cluster"                   ; t_id = 0x0F43B675L; t_innards = T_elt   };
	{t_name = "Timecode"                  ; t_id = 0x67L      ; t_innards = T_uint  };
	{t_name = "SilentTracks"              ; t_id = 0x1854L    ; t_innards = T_elt   };
	{t_name = "SilentTrackNumber"         ; t_id = 0x18D7L    ; t_innards = T_uint  };
	{t_name = "Position"                  ; t_id = 0x27L      ; t_innards = T_uint  };
	{t_name = "PrevSize"                  ; t_id = 0x2BL      ; t_innards = T_uint  };
	{t_name = "BlockGroup"                ; t_id = 0x20L      ; t_innards = T_elt   };
	{t_name = "Block"                     ; t_id = 0x21L      ; t_innards = T_binary};
	{t_name = "BlockAdditions"            ; t_id = 0x35A1L    ; t_innards = T_elt   };
	{t_name = "BlockMore"                 ; t_id = 0x26L      ; t_innards = T_elt   };
	{t_name = "BlockAddID"                ; t_id = 0x5EL      ; t_innards = T_uint  };
	{t_name = "BlockAdditional"           ; t_id = 0x25L      ; t_innards = T_binary};
	{t_name = "BlockDuration"             ; t_id = 0x1BL      ; t_innards = T_uint  };
	{t_name = "ReferencePriority"         ; t_id = 0x7AL      ; t_innards = T_uint  };
	{t_name = "ReferenceBlock"            ; t_id = 0x7BL      ; t_innards = T_sint  };
	{t_name = "Slices"                    ; t_id = 0x0EL      ; t_innards = T_elt   };
	{t_name = "TimeSlice-DEAD"            ; t_id = 0x68L      ; t_innards = T_elt   };
	{t_name = "LaceNumber-DEAD"           ; t_id = 0x4CL      ; t_innards = T_uint  };
	{t_name = "Duration"                  ; t_id = 0x4FL      ; t_innards = T_uint  };
	{t_name = "Tracks"                    ; t_id = 0x0654AE6BL; t_innards = T_elt   };
	{t_name = "TrackEntry"                ; t_id = 0x2EL      ; t_innards = T_elt   };
	{t_name = "TrackNumber"               ; t_id = 0x57L      ; t_innards = T_uint  };
	{t_name = "TrackUID"                  ; t_id = 0x33C5L    ; t_innards = T_uint  };
	{t_name = "TrackType"                 ; t_id = 0x03L      ; t_innards = T_uint  };
	{t_name = "FlagEnabled"               ; t_id = 0x39L      ; t_innards = T_uint  };
	{t_name = "FlagDefault"               ; t_id = 0x08L      ; t_innards = T_uint  };
	{t_name = "FlagForced"                ; t_id = 0x15AAL    ; t_innards = T_uint  };
	{t_name = "FlagLacing"                ; t_id = 0x1CL      ; t_innards = T_uint  };
	{t_name = "MinCache"                  ; t_id = 0x2DE7L    ; t_innards = T_uint  };
	{t_name = "MaxCache"                  ; t_id = 0x2DF8L    ; t_innards = T_uint  };
	{t_name = "DefaultDuration"           ; t_id = 0x03E383L  ; t_innards = T_uint  };
	{t_name = "TrackTimecodeScale"        ; t_id = 0x03314FL  ; t_innards = T_float };
	{t_name = "MaxBlockAdditionID"        ; t_id = 0x15EEL    ; t_innards = T_uint  };
	{t_name = "Name"                      ; t_id = 0x136EL    ; t_innards = T_utf8  };
	{t_name = "Language"                  ; t_id = 0x02B59CL  ; t_innards = T_string};
	{t_name = "CodecID"                   ; t_id = 0x06L      ; t_innards = T_string};
	{t_name = "CodecPrivate"              ; t_id = 0x23A2L    ; t_innards = T_binary};
	{t_name = "CodecName"                 ; t_id = 0x058688L  ; t_innards = T_utf8  };
	{t_name = "TrackOverlay"              ; t_id = 0x2FABL    ; t_innards = T_uint  };
	{t_name = "TrackTranslate"            ; t_id = 0x2624L    ; t_innards = T_elt   };
	{t_name = "TrackTranslateEditionUID"  ; t_id = 0x26FCL    ; t_innards = T_uint  };
	{t_name = "TrackTranslateCodec"       ; t_id = 0x26BFL    ; t_innards = T_uint  };
	{t_name = "TrackTranslateTrackID"     ; t_id = 0x26A5L    ; t_innards = T_binary};
	{t_name = "Video"                     ; t_id = 0x60L      ; t_innards = T_elt   };
	{t_name = "FlagInterlaced"            ; t_id = 0x1AL      ; t_innards = T_uint  };
	{t_name = "PixelWidth"                ; t_id = 0x30L      ; t_innards = T_uint  };
	{t_name = "PixelHeight"               ; t_id = 0x3AL      ; t_innards = T_uint  };
	{t_name = "PixelCropBottom"           ; t_id = 0x14AAL    ; t_innards = T_uint  };
	{t_name = "PixelCropTop"              ; t_id = 0x14BBL    ; t_innards = T_uint  };
	{t_name = "PixelCropLeft"             ; t_id = 0x14CCL    ; t_innards = T_uint  };
	{t_name = "PixelCropRight"            ; t_id = 0x14DDL    ; t_innards = T_uint  };
	{t_name = "DisplayWidth"              ; t_id = 0x14B0L    ; t_innards = T_uint  };
	{t_name = "DisplayHeight"             ; t_id = 0x14BAL    ; t_innards = T_uint  };
	{t_name = "DisplayUnit"               ; t_id = 0x14B2L    ; t_innards = T_uint  };
	{t_name = "AspectRatioType"           ; t_id = 0x14B3L    ; t_innards = T_uint  };
	{t_name = "ColorSpace"                ; t_id = 0x0EB524L  ; t_innards = T_binary};
	{t_name = "Audio"                     ; t_id = 0x61L      ; t_innards = T_elt   };
	{t_name = "SamplingFrequency"         ; t_id = 0x35L      ; t_innards = T_float };
	{t_name = "OutputSamplingFrequency"   ; t_id = 0x38B5L    ; t_innards = T_float };
	{t_name = "Channels"                  ; t_id = 0x1FL      ; t_innards = T_uint  };
	{t_name = "BitDepth"                  ; t_id = 0x2264L    ; t_innards = T_uint  };
	{t_name = "ContentEncodings"          ; t_id = 0x2D80L    ; t_innards = T_elt   };
	{t_name = "ContentEncoding"           ; t_id = 0x2240L    ; t_innards = T_elt   };
	{t_name = "ContentEncodingOrder"      ; t_id = 0x1031L    ; t_innards = T_uint  };
	{t_name = "ContentEncodingScope"      ; t_id = 0x1032L    ; t_innards = T_uint  };
	{t_name = "ContentEncodingType"       ; t_id = 0x1033L    ; t_innards = T_uint  };
	{t_name = "ContentCompression"        ; t_id = 0x1034L    ; t_innards = T_elt   };
	{t_name = "ContentCompAlgo"           ; t_id = 0x0254L    ; t_innards = T_uint  };
	{t_name = "ContentCompSettings"       ; t_id = 0x0255L    ; t_innards = T_binary};
	{t_name = "ContentEncryption"         ; t_id = 0x1035L    ; t_innards = T_elt   };
	{t_name = "ContentEncAlgo"            ; t_id = 0x07E1L    ; t_innards = T_uint  };
	{t_name = "ContentEncKeyID"           ; t_id = 0x07E2L    ; t_innards = T_binary};
	{t_name = "ContentSignature"          ; t_id = 0x07E3L    ; t_innards = T_binary};
	{t_name = "ContentSigKeyID"           ; t_id = 0x07E4L    ; t_innards = T_binary};
	{t_name = "ContentSigAlgo"            ; t_id = 0x07E5L    ; t_innards = T_uint  };
	{t_name = "ContentSigHashAlgo"        ; t_id = 0x07E6L    ; t_innards = T_uint  };
	{t_name = "Cues"                      ; t_id = 0x0C53BB6BL; t_innards = T_elt   };
	{t_name = "Attachments"               ; t_id = 0x0941A469L; t_innards = T_elt   };
	{t_name = "Chapters"                  ; t_id = 0x0043A770L; t_innards = T_elt   };
	{t_name = "EditionEntry"              ; t_id = 0x05B9L    ; t_innards = T_elt   };
	{t_name = "EditionUID"                ; t_id = 0x05BCL    ; t_innards = T_uint  };
	{t_name = "EditionFlagHidden"         ; t_id = 0x05BDL    ; t_innards = T_uint  };
	{t_name = "EditionFlagDefault"        ; t_id = 0x05DBL    ; t_innards = T_uint  };
	{t_name = "EditionFlagOrdered"        ; t_id = 0x05DDL    ; t_innards = T_uint  };
	{t_name = "ChapterAtom"               ; t_id = 0x36L      ; t_innards = T_elt   };
	{t_name = "ChapterUID"                ; t_id = 0x33C4L    ; t_innards = T_uint  };
	{t_name = "ChapterTimeStart"          ; t_id = 0x11L      ; t_innards = T_uint  };
	{t_name = "ChapterTimeEnd"            ; t_id = 0x12L      ; t_innards = T_uint  };
	{t_name = "ChapterFlagHidden"         ; t_id = 0x18L      ; t_innards = T_uint  };
	{t_name = "ChapterFlagEnabled"        ; t_id = 0x0598L    ; t_innards = T_uint  };
	{t_name = "ChapterSegmentUID"         ; t_id = 0x2E67L    ; t_innards = T_binary};
	{t_name = "ChapterPhysicalEquiv"      ; t_id = 0x23C3L    ; t_innards = T_uint  };
	{t_name = "ChapterTrack"              ; t_id = 0x0FL      ; t_innards = T_elt   };
	{t_name = "ChapterTrackNumber"        ; t_id = 0x09L      ; t_innards = T_uint  };
	{t_name = "ChapterDisplay"            ; t_id = 0x00L      ; t_innards = T_elt   };
	{t_name = "ChapString"                ; t_id = 0x05L      ; t_innards = T_utf8  };
	{t_name = "ChapLanguage"              ; t_id = 0x037CL    ; t_innards = T_string};
	{t_name = "ChapCountry"               ; t_id = 0x037EL    ; t_innards = T_string};
	{t_name = "ChapProcess"               ; t_id = 0x2944L    ; t_innards = T_elt   };
	{t_name = "ChapProcessCodecID"        ; t_id = 0x2955L    ; t_innards = T_uint  };
	{t_name = "ChapProcessPrivate"        ; t_id = 0x050DL    ; t_innards = T_binary};
	{t_name = "ChapProcessCommand"        ; t_id = 0x2911L    ; t_innards = T_elt   };
	{t_name = "ChapProcessTime"           ; t_id = 0x9222L    ; t_innards = T_uint  };
	{t_name = "ChapProcessData"           ; t_id = 0x2933L    ; t_innards = T_binary};
	{t_name = "Tags"                      ; t_id = 0x0254C367L; t_innards = T_elt   };
	{t_name = "Tag"                       ; t_id = 0x3373L    ; t_innards = T_elt   };
	{t_name = "Targets"                   ; t_id = 0x23C0L    ; t_innards = T_elt   };
	{t_name = "TargetTypeValue"           ; t_id = 0x28CAL    ; t_innards = T_uint  };
	{t_name = "TargetType"                ; t_id = 0x23CAL    ; t_innards = T_string};
	{t_name = "TrackUID"                  ; t_id = 0x23C5L    ; t_innards = T_uint  };
	{t_name = "EditionUID"                ; t_id = 0x23C9L    ; t_innards = T_uint  };
	{t_name = "ChapterUID"                ; t_id = 0x23C4L    ; t_innards = T_uint  };
	{t_name = "AttachmentUID"             ; t_id = 0x23C6L    ; t_innards = T_uint  };
	{t_name = "SimpleTag"                 ; t_id = 0x27C8L    ; t_innards = T_elt   };
	{t_name = "TagName"                   ; t_id = 0x05A3L    ; t_innards = T_utf8  };
	{t_name = "TagLanguage"               ; t_id = 0x047AL    ; t_innards = T_string};
	{t_name = "TagDefault"                ; t_id = 0x04B4L    ; t_innards = T_uint  };
	{t_name = "TagString"                 ; t_id = 0x0487L    ; t_innards = T_utf8  };
	{t_name = "TagBinary"                 ; t_id = 0x0485L    ; t_innards = T_binary};

(*
	{t_name = "EBML"; t_id = 0xL; t_innards = T_; t_parser = None};
*)
];;

let input_string f len =
	let out_str = String.create len in
(*	Printf.printf "     Geb7?b %d @ %Ld\n" len (LargeFile.pos_in f);*)
	really_input f out_str 0 len;
(*	ignore (input f out_str 0 len);*)
	out_str
;;

let input_string_int64 f len =
	if len > (!| Sys.max_string_length) then raise (Invalid_argument "String.create");
	input_string f (!- len)
;;

let rec input_big_string f len =
	if len <= !| Sys.max_string_length then (
		input_string f (!- len) :: []
	) else (
		let str_now = input_string f Sys.max_string_length in
		str_now :: (input_big_string f (len -| (!| Sys.max_string_length)))
	)
;;

let rec parse_element f dtd_id parse_to =
	let id = read_id f in
	let (len, writelen) = (
		let temp = read_len f in
		match temp with
		| None -> (parse_to -| LargeFile.pos_in f, -1L) (* Read up to the end of the range *)
		| Some x -> (x,x)
	) in
	if Hashtbl.mem dtd_id id then (
		let def = Hashtbl.find dtd_id id in
		let elt_id = {id_name = def.t_name; id_id = id} in
		match def.t_innards with
		| T_uint     -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_uint     (read_uint f len)}]
		| T_sint     -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_sint     (read_sint f len)}]
		| T_float    -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_float    (read_float f len)}]
		| T_string   -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_string   (input_string_int64 f len)}]
		| T_utf8     -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_utf8     (input_string_int64 f len)}]
		| T_date     -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_date     (read_sint f len)}]
		| T_binary   -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_binary   (input_big_string f len)}]
		| T_unparsed -> [{elt_id = elt_id; elt_len = writelen; elt_innards = In_unparsed (input_big_string f len)}]
		| T_skipped  -> (LargeFile.seek_in f (LargeFile.pos_in f +| len); [])
		| T_elt      -> (
			let go_to = LargeFile.pos_in f +| len in
			let rec do_another () = (
				if LargeFile.pos_in f < go_to then (
					match parse_element f dtd_id go_to with
					| [a] -> a :: do_another ()
					|  b  -> b @  do_another ()
				) else (
					[]
				)
			) in
			let elements = do_another () in
			let len_total = List.fold_left (fun so_far gnu -> so_far +| gnu.elt_len +| len_length gnu.elt_len +| id_length gnu.elt_id.id_id) 0L elements in
			[{elt_id = elt_id; elt_len = (if writelen < 0L then writelen else len_total); elt_innards = In_elt elements}]
		)
		| T_uint_f     g -> g (elt_id, writelen, (read_uint f len))
		| T_sint_f     g -> g (elt_id, writelen, (read_sint f len))
		| T_float_f    g -> g (elt_id, writelen, (read_float f len))
		| T_string_f   g -> g (elt_id, writelen, (input_string_int64 f len))
		| T_utf8_f     g -> g (elt_id, writelen, (input_string_int64 f len))
		| T_date_f     g -> g (elt_id, writelen, (read_sint f len))
		| T_binary_f   g -> g (elt_id, writelen, (input_big_string f len))
		| T_unparsed_f g -> g (elt_id, writelen, (input_big_string f len))
		| T_skipped_f  g -> (LargeFile.seek_in f (LargeFile.pos_in f +| len); g (elt_id, writelen, ()))
		| T_elt_f      g -> (
			let go_to = LargeFile.pos_in f +| len in
			let rec do_another () = (
				if LargeFile.pos_in f < go_to then (
					match parse_element f dtd_id go_to with
					| [a] -> a :: do_another ()
					|  b  -> b @  do_another ()
				) else (
					[]
				)
			) in
			let elements = do_another () in
			let len_total = List.fold_left (fun so_far gnu -> so_far +| gnu.elt_len +| len_length gnu.elt_len +| id_length gnu.elt_id.id_id) 0L elements in
			g (elt_id, (if writelen < 0L then writelen else len_total), elements)
		)

(*
		{
			elt_id = {id_name = elt.t_name; id_id = id};
			elt_len = len;
			elt_innards = (
				match elt.t_innards with
				| T_uint     None -> In_uint (read_uint f len)
				| T_sint     None -> In_sint (read_sint f len)
				| T_float    None -> In_float (read_float f len)
				| T_string   None -> In_string (input_string_int64 f len)
				| T_utf8     None -> In_utf8 (input_string_int64 f len)
				| T_date     None -> In_date (read_sint f len)
				| T_elt      None -> In_elt (
						let go_to = LargeFile.pos_in f +| len in
						let rec do_another () = (
							if LargeFile.pos_in f < go_to then (
								let elt_now = parse_element f dtd_id go_to in
								elt_now :: do_another ()
							) else (
								[]
							)
						) in
						do_another ()
					)
				| T_binary   None -> In_binary (input_big_string f len)
				| T_unparsed None -> In_unparsed (input_big_string f len)
				| T_skipped  None -> (LargeFile.seek_in f (LargeFile.pos_in f +| len); In_skipped)

				| T_uint     (Some fn) -> In_uint (fn (read_uint f len))
				| T_sint     (Some fn) -> In_sint (fn (read_sint f len))
				| T_float    (Some fn) -> In_float (fn (read_float f len))
				| T_string   (Some fn) -> In_string (fn (input_string_int64 f len))
				| T_utf8     (Some fn) -> In_utf8 (fn (input_string_int64 f len))
				| T_date     (Some fn) -> In_date (fn (read_sint f len))
				| T_elt      (Some fn) -> In_elt (fn (
						let go_to = LargeFile.pos_in f +| len in
						let rec do_another () = (
							if LargeFile.pos_in f < go_to then (
								let elt_now = parse_element f dtd_id go_to in
								elt_now :: do_another ()
							) else (
								[]
							)
						) in
						do_another ()
					))
				| T_binary   (Some fn) -> In_binary (fn (input_big_string f len))
				| T_unparsed (Some fn) -> In_unparsed (fn (input_big_string f len))
				| T_skipped  (Some fn) -> (LargeFile.seek_in f (LargeFile.pos_in f +| len); fn (); In_skipped)
			)
		}
*)
	) else (
		LargeFile.seek_in f (LargeFile.pos_in f +| len);
		[{
			elt_id = {id_name = "UNKNOWN ELEMENT"; id_id = id};
			elt_len = len;
			elt_innards = In_unknown
		}]
	)
;;

let parse_with_dtd filename dtd =
	let f = open_in_bin filename in

	(* Parse the DTD and index by both ID and name *)
	let dtd_length = List.length dtd in
	let dtd_name = Hashtbl.create dtd_length in
	let dtd_id   = Hashtbl.create dtd_length in
	List.iter (fun entry ->
		Hashtbl.add dtd_name entry.t_name entry;
		Hashtbl.add dtd_id entry.t_id entry;
	) dtd;
(*
	Hashtbl.iter (fun a b ->
		Printf.printf "%s\n" a
	) dtd_name;
*)
	let file_length = LargeFile.in_channel_length f in

	let rec do_another () = (
		if LargeFile.pos_in f < file_length then (
(*
			let elt_now = parse_element f dtd_id file_length in
			elt_now :: do_another ()
*)
			match parse_element f dtd_id file_length with
			| [a] -> (a :: do_another ())
			|  b  -> (b @ do_another ())
		) else (
			[]
		)
	) in
	let x = do_another () in
	close_in f;
	x
(*	parse_element f dtd_id file_length*)
;;

let rec print_element ?(indent="") element =
	Printf.printf "%s%s (0x%LX):\n" indent element.elt_id.id_name element.elt_id.id_id; (* That's a lot of IDs... *)
	Printf.printf "%s %Ld\n" indent element.elt_len;
	match element.elt_innards with
	| In_sint x -> Printf.printf "%s SINT %Ld\n" indent x
	| In_uint x -> Printf.printf "%s UINT %Ld\n" indent x
	| In_float x -> Printf.printf "%s FLOAT %f\n" indent x
	| In_string x -> Printf.printf "%s STRING %S\n" indent x
	| In_utf8 x -> Printf.printf "%s UTF8 %S\n" indent x
	| In_date x -> Printf.printf "%s DATE %Ld\n" indent x
	| In_elt x -> (Printf.printf "%s ELEMENTS\n" indent; List.iter (fun y -> print_element ~indent:(indent ^ "  ") y) x)
	| In_binary (x :: y) -> Printf.printf "%s BINARY %S\n" indent (if String.length x < 80000 then x else "...")
	| In_binary [] -> Printf.printf "%s BINARY (nothing?)\n" indent
	| In_unparsed x -> Printf.printf "%s UNPARSED\n" indent
	| In_skipped -> Printf.printf "%s SKIPPED\n" indent
	| In_unknown -> Printf.printf "%s UNKNOWN ELEMENT!\n" indent
;;

let print_element_list ?(indent="") element_list =
	List.iter (fun elt ->
		print_element ~indent:indent elt
	) element_list
;;
(*
let rec adjust_lengths ?(fix_unknown=false) element_list =
	let len_innards = List.fold_left (fun so_far elt_now ->
		if elt_now.elt_len >= 0L || fix_unknown then (
			let new_len = match elt_now.elt_innards with
				| In_sint x -> len_sint x
				| In_uint x -> len_uint x
				| In_float x -> elt_now.elt_len (* Leave float lengths alone *)
				| In_string x -> !| (String.length x)
				| In_utf8 x -> !| (String.length x)
				| In_date x -> len_sint x
				| In_skipped | In_unknown -> 0L (* Throw out whatever was in skipped or unknown elements *)
				| In_binary x | In_unparsed x -> ( (* Iterate over the string list *)
					List.fold_left (fun sofar newstr -> sofar +| (!| (String.length newstr))) 0L x
				)
				| In_elt x -> (adjust_lengths x)
			in
			elt_now.elt_len <- new_len;
			match elt_now.elt_innards with
			| In_skipped | In_unknown -> so_far
			| _ -> so_far +| new_len +| (len_length elt_now.elt_len) +| (len_length elt_now.elt_id.id_id)
		) else (
			(* Not changing the "unknown" length *)
			match elt_now.elt_innards with
			| In_elt x -> 
		)
	) 0L element_list in
	len_innards;;
*)
let rec render_elements element_list =
	let rendered_list = List.map (fun elt ->
		let innards_string_list = match elt.elt_innards with
			| In_sint x -> [write_sint_to_string x]
			| In_uint x -> [write_uint_to_string x]
			| In_float x -> [write_float_to_string x elt.elt_len]
			| In_string x -> [x]
			| In_utf8 x -> [x]
			| In_date x -> [write_sint_to_string x]
			| In_skipped | In_unknown -> []
			| In_binary x | In_unparsed x -> x
			| In_elt x -> (render_elements x)
		in
		match elt.elt_innards with
		| In_skipped | In_unknown -> innards_string_list
		| _ -> (write_length_to_string elt.elt_id.id_id) :: (write_length_to_string elt.elt_len) :: innards_string_list
	) element_list in
	List.flatten rendered_list;;








(*
type
	value_t = ValSint of int64 | ValUint of int64 | ValFloat of float | ValString of string | ValUTF8 of string | ValDate of int64 | ValElements of (int, elementGuts_t) Hashtbl.t | ValBinary of string | ValBlock of block_t | ValSkipped and
	elementGuts_t = {headerSize : int; elementPos : int64; elementSize : int64; elementValue : value_t} and
(* Old stuff...
	nodeGuts_t = {headerSize : int; pos : int64; size : int64; value : value_t} and
	node_t = NodeList of (int64, nodeGuts_t) Hashtbl.t | NodeGuts of nodeGuts_t and
*)
	lacing_t = LacingNone | LacingXiph | LacingEBML | LacingFixed and
	frame_t = {framePos : int64; frameSize : int64} and (* Int64 is probably overkill for a frame size, but I'm pretty sure there's no upper bound on frame sizes. *)
	block_t = {lacing : lacing_t; timecode : int; track : int64; invisible : bool; frames : frame_t array};;

(* Meta-types. They specify how types are to be read from a file *)
type value_t_t = Sint | Uint | Float | String | UTF8 | Date | Elements | Binary | Block;; (* OK, so "Block" really belongs in the Matroska-specific part. It's defined here cuz it's handy. *)
type traverse_t = No | Yes | All;; (* "All" Traverses the current element and all children *)
type pos_t = PosAny | PosTop | PosSome of int list;; (* "PosAny" means that it's allowed anywhere, "PosTop" means that it's a top-level element, and "PosSome" is a list of IDs which can be the immediate parent of the current element *)
type parent_t = ParentTop | ParentElement of int | ParentAny;; (* This is to be passed as the "parent" object type *)
type node_t_t  = {id : int; name : string; value : value_t_t; multi : bool; traverse : traverse_t; pos : pos_t};; (* How to handle a node_t type *)
*)
