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

type buf_t = {buf : Buffer2.t; mutable pos : int};;
type 'a perhaps_t = One of 'a | Need_more of int;;

exception Invalid_integer;;
exception Invalid_float_length of int;;
exception Invalid_ID;;

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

let nth_int b n = Char.code (Buffer2.nth b n);;
let pos_int b = (
	let a = nth_int b.buf b.pos in
	b.pos <- b.pos + 1;
	a
);;

(*********************)
(* Reading functions *)
(*********************)

let read_uint len b =
	if len + b.pos > Buffer2.length b.buf then (
		Need_more (len + b.pos - Buffer2.length b.buf)
	) else (
		let rec read_rec len so_far = (
			if len = 0 then (
				so_far
			) else (
				let c = pos_int b in
				read_rec (pred len) ((so_far <| 8) ||| ( !| c ))
			)
		) in
		One (read_rec len 0L)
	)
;;

let read_sint len b =
	if len + b.pos > Buffer2.length b.buf then (
		Need_more (len + b.pos - Buffer2.length b.buf)
	) else (
		let start = (!| (pos_int b)) <| 56 >|- 56 in (* Puts the sign from the first byte into all the bytes *)
		let rec read_rec len so_far = (
			if len = 0 then (
				so_far
			) else (
				let c = pos_int b in
				read_rec (pred len) ((so_far <| 8) ||| ( !| c ))
			)
		) in
		One (read_rec (pred len) start)
	)
;;

let read_xiph b =
	let rec read_xiph_rec so_far on = (
		match (nth_int b.buf on, on = Buffer2.length b.buf - 1) with
		| (255, true ) -> Need_more 0 (* No idea how much more, don't bother to update pos *)
		| (255, false) -> read_xiph_rec (so_far +| 255L) (succ on)
		| ( x ,   _  ) -> (
			b.pos <- on + 1;
			One (so_far +| ( !| x ))
		)
	) in
	read_xiph_rec 0L b.pos
;;

let read_len b =
	if b.pos >= Buffer2.length b.buf then (
		Need_more 0
	) else (
		let b1 = nth_int b.buf b.pos in
		let (more_bytes, initial_byte, escape_len) = (
			if b1 land 0x80 <> 0 then (
				(0, (!| (b1 land 0x7F)) <| 0, 0x7FL)
			) else if b1 land 0x40 <> 0 then (
				(1, (!| (b1 land 0x3F)) <| 1, 0x3FFFL)
			) else if b1 land 0x20 <> 0 then (
				(2, (!| (b1 land 0x1F)) <| 2, 0x1FFFFFL)
			) else if b1 land 0x10 <> 0 then (
				(3, (!| (b1 land 0x0F)) <| 3, 0x0FFFFFFFL)
			) else if b1 land 0x08 <> 0 then (
				(4, (!| (b1 land 0x07)) <| 4, 0x07FFFFFFFFL)
			) else if b1 land 0x04 <> 0 then (
				(5, (!| (b1 land 0x03)) <| 5, 0x03FFFFFFFFFFL)
			) else if b1 land 0x02 <> 0 then (
				(6, (!| (b1 land 0x01)) <| 6, 0x01FFFFFFFFFFFFL)
			) else if b1 land 0x01 <> 0 then (
				(7, 0L,                       0x00FFFFFFFFFFFFFFL)
			) else (
				raise Invalid_integer
			)
		) in
		if b.pos + more_bytes >= Buffer2.length b.buf then (
			Need_more (b.pos + more_bytes - Buffer2.length b.buf + 1)
		) else (
			b.pos <- b.pos + 1;
			match read_uint more_bytes b with
			| Need_more x -> failwith "Matroskastream.read_len" (* We already checked to make sure there are enough bytes *)
			| One x -> (
				let num = (initial_byte ||| x) in
				if num = escape_len then (
					One None (* Ermm... This makes sense. Trust me. *)
				) else (
					One (Some num) (* It's all because lengths may be undefined even if there are enough bytes to determine the value *)
				)
			)
		)
	)
;;

let read_float len b =
	if len <> 4 && len <> 8 then (
		raise (Invalid_float_length len)
	) else (
		match read_uint len b with
		| Need_more x -> Need_more x
		| One x -> (
			if len = 4 then (
				One (Int32.float_of_bits (Int64.to_int32 x))
			) else (
				One (Int64.float_of_bits x)
			)
		)
	)
;;

let read_id b =
	if b.pos >= Buffer2.length b.buf then (
		Need_more 0
	) else (
		let b1 = nth_int b.buf b.pos in
		let (more_bytes, initial_byte) = (
			if b1 land 0x80 <> 0 then (
				(0, (!| (b1 land 0x7F)) <| 0)
			) else if b1 land 0x40 <> 0 then (
				(1, (!| (b1 land 0x3F)) <| 1)
			) else if b1 land 0x20 <> 0 then (
				(2, (!| (b1 land 0x1F)) <| 2)
			) else if b1 land 0x10 <> 0 then (
				(3, (!| (b1 land 0x0F)) <| 3)
			) else (
				raise Invalid_ID
			)
		) in
		if b.pos + more_bytes >= Buffer2.length b.buf then (
			Need_more (b.pos + more_bytes - Buffer2.length b.buf + 1)
		) else (
			b.pos <- b.pos + 1;
			match read_uint more_bytes b with
			| Need_more x -> failwith "Matroskastream.read_id"
			| One x -> (
				One (initial_byte ||| x)
			)
		)
	)
;;


(***********)
(* OBJECT! *)
(***********)
class matroskastream =
	object (o)
		val buffer = {buf = Buffer2.create 8192; pos = 0}
		
		
