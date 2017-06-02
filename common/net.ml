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

let debug_file = false;;


(************************)
(* Let's try that again *)
(************************)
let unexpected_packet (s,t) = (Failure (Printf.sprintf "Tag %S (length %d) was not expected here" s (String.length t)));;

let rec really_recv d s off len =
	if len <= 0 then () else (
(*
		let r = (try
			Unix.recv d s off len []
		with
			Unix.Unix_error (Unix.EUNKNOWNERR 0,_,_) -> (print "Got a strange error while receiving. Check output file!\n"; len)
		) in
*)
		let r = Unix.recv d s off len [] in
		if r = 0 then (
			raise End_of_file
		) else (
			really_recv d s (off + r) (len - r)
		)
	)
;;

(* I don't know if this is needed, but I don't like ignoring stuff *)
let rec really_send d s off len =
	if len <= 0 then () else (
(*
		let r = (try
			Unix.send d s off len []
		with
			Unix.Unix_error (Unix.EUNKNOWNERR 0,_,_) -> (print "Got a strange error while sending. Check output file!\n"; len)
		) in
*)
		let r = Unix.send d s off len [] in
		if r = 0 then (
			raise End_of_file
		) else (
			really_send d s (off + r) (len - r)
		)
	)
;;

let recv_base d =
	let tag = String.create 4 in
	really_recv d tag 0 4;
	let len_bin = String.create 4 in
	really_recv d len_bin 0 4;
	let len = unpackN len_bin 0 in
	let str = String.create len in
	really_recv d str 0 len;
	
	(tag, str)
;;

let send d tag str =
	let len_bin = packN (String.length str) in
(*
	really_send d tag 0 4;
	really_send d len_bin 0 4;
	really_send d str 0 (String.length str);
*)
	really_send d (tag ^ len_bin ^ str) 0 (String.length str + 8);

	(* Now see if it was really received *)
	(
		let (tag,str) = recv_base d in
		match (tag,str) with
		| ("RECV","") -> () (* OK! *)
		| x -> raise (unexpected_packet x)
	);
;;

(* Sends a pre-formatted string, with headers already in place *)
let send_full d ?len s =
	(match len with
		| None -> (
			(* Just output *)
			really_send d s 0 (String.length s)
		)
		| Some x -> (
			let len_bin = packN x in
			String.blit len_bin 0 s 4 4;
			really_send d s 0 (x + 8)
		)
	);
	(
		let (tag,str) = recv_base d in
		match (tag,str) with
		| ("RECV","") -> () (* OK! *)
		| x -> raise (unexpected_packet x)
	);
;;

let recv d =
	let (tag,str) = recv_base d in
	really_send d "RECV\x00\x00\x00\x00" 0 8;
	(tag,str)
;;
