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
open Net;;
open Huff;;


Printf.printf "HUFF:\n";;
let _ = (

	let y_data = [| (1,1);(3,2);(4,2);(6,5);(7,7);(7,8);(8,9);(8,10);(9,11);(9,12);(9,13);(9,14);(10,13);(10,14);(10,15);(10,16);(11,15);(11,16);(11,17);(11,18);(11,19);(11,20);(12,16);(12,17);(12,18);(12,19);(12,20);(12,21);(12,22);(13,16);(13,17);(13,18);(13,19);(13,20);(13,21);(13,22);(13,23);(14,16);(14,17);(14,18);(14,19);(14,20);(14,21);(14,22);(14,23);(15,16);(15,17);(15,18);(15,19);(15,20);(15,21);(15,22);(15,23);(16,16);(16,17);(16,18);(16,19);(16,20);(16,21);(16,22);(16,23);(17,16);(17,17);(17,18);(17,19);(17,20);(17,21);(17,22);(17,23);(18,16);(18,17);(18,18);(18,19);(18,20);(18,21);(18,22);(18,23);(19,16);(19,17);(19,18);(19,19);(19,20);(19,21);(19,22);(19,23);(20,15);(20,16);(20,17);(20,18);(20,19);(20,20);(20,21);(20,22);(21,14);(21,15);(21,16);(21,17);(21,18);(21,19);(21,20);(21,21);(22,13);(22,14);(22,15);(22,16);(22,17);(22,18);(22,19);(22,20);(23,10);(23,11);(23,12);(23,13);(23,14);(23,15);(23,16);(23,17);(24,3);(24,4);(24,5);(24,6);(24,7);(24,8);(24,9);(24,10);(25,0);(25,1);(25,2);(25,3);(25,4);(25,5);(24,11);(24,12);(24,13);(24,14);(24,15);(24,16);(24,17);(24,18);(24,19);(23,18);(23,19);(23,20);(23,21);(23,22);(23,23);(23,24);(23,25);(22,21);(22,22);(22,23);(22,24);(22,25);(22,26);(22,27);(21,22);(21,23);(21,24);(21,25);(21,26);(21,27);(21,28);(21,29);(20,23);(20,24);(20,25);(20,26);(20,27);(20,28);(20,29);(20,30);(20,31);(19,24);(19,25);(19,26);(19,27);(19,28);(19,29);(19,30);(19,31);(18,24);(18,25);(18,26);(18,27);(18,28);(18,29);(18,30);(18,31);(17,24);(17,25);(17,26);(17,27);(17,28);(17,29);(17,30);(17,31);(16,24);(16,25);(16,26);(16,27);(16,28);(16,29);(16,30);(16,31);(15,24);(15,25);(15,26);(15,27);(15,28);(15,29);(15,30);(15,31);(14,24);(14,25);(14,26);(14,27);(14,28);(14,29);(14,30);(14,31);(13,24);(13,25);(13,26);(13,27);(13,28);(13,29);(13,30);(13,31);(12,23);(12,24);(12,25);(12,26);(12,27);(12,28);(12,29);(11,21);(11,22);(11,23);(11,24);(11,25);(10,17);(10,18);(10,19);(10,20);(10,21);(9,15);(9,16);(9,17);(8,11);(8,12);(8,13);(7,9);(6,6);(6,7);(4,3);(3,3) |] in
	let u_data = [| (1,1);(3,1);(5,2);(6,2);(8,3);(8,4);(9,4);(10,4);(10,5);(11,5);(12,5);(12,6);(12,7);(13,6);(13,7);(14,6);(14,7);(14,8);(15,7);(15,8);(16,7);(16,8);(16,9);(16,10);(17,8);(17,9);(17,10);(18,8);(18,9);(18,10);(18,11);(19,8);(19,9);(19,10);(19,11);(20,8);(20,9);(20,10);(20,11);(21,9);(21,10);(21,11);(22,9);(22,10);(22,11);(22,12);(22,13);(23,10);(23,11);(23,12);(23,13);(24,11);(24,12);(24,13);(24,14);(25,12);(25,13);(25,14);(25,15);(25,16);(26,13);(26,14);(26,15);(26,16);(26,17);(26,18);(27,16);(27,17);(27,18);(27,19);(27,20);(28,18);(28,19);(28,20);(28,21);(28,22);(28,23);(28,24);(29,21);(29,22);(29,23);(29,24);(29,25);(29,26);(29,27);(30,23);(30,24);(30,25);(30,26);(30,27);(30,28);(30,29);(30,30);(30,31);(30,32);(31,21);(31,22);(31,23);(31,24);(31,25);(31,26);(31,27);(31,28);(31,29);(31,30);(31,31);(31,32);(32,0);(32,1);(32,2);(32,3);(32,4);(32,5);(32,6);(32,7);(32,8);(32,9);(32,10);(32,11);(32,12);(32,13);(32,14);(32,15);(32,16);(32,17);(32,18);(32,19);(32,20);(32,21);(32,22);(32,23);(32,24);(32,25);(32,26);(32,27);(32,28);(32,29);(32,30);(32,31);(32,32);(32,33);(32,34);(32,35);(32,36);(32,37);(32,38);(32,39);(32,40);(32,41);(31,33);(31,34);(31,35);(31,36);(31,37);(31,38);(31,39);(31,40);(31,41);(31,42);(31,43);(31,44);(31,45);(30,33);(30,34);(30,35);(30,36);(30,37);(30,38);(30,39);(30,40);(30,41);(29,28);(29,29);(29,30);(29,31);(29,32);(29,33);(29,34);(29,35);(28,25);(28,26);(28,27);(28,28);(28,29);(28,30);(28,31);(27,21);(27,22);(27,23);(27,24);(27,25);(26,19);(26,20);(26,21);(26,22);(26,23);(25,17);(25,18);(25,19);(25,20);(25,21);(24,15);(24,16);(24,17);(24,18);(24,19);(23,14);(23,15);(23,16);(23,17);(22,14);(22,15);(22,16);(22,17);(21,12);(21,13);(21,14);(21,15);(20,12);(20,13);(20,14);(20,15);(19,12);(19,13);(19,14);(19,15);(18,12);(18,13);(18,14);(18,15);(17,11);(17,12);(17,13);(16,11);(16,12);(16,13);(15,9);(15,10);(15,11);(14,9);(14,10);(14,11);(13,8);(13,9);(12,8);(12,9);(11,6);(11,7);(10,6);(10,7);(9,5);(8,5);(7,3);(6,3);(5,3);(2,1) |] in


	(* Let's add some random data *)
	let bin = new_huff_buff 16777211 in

	let test_array = [| 0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;0;1;2;3;4;5;6;7;8;9;10 |] in
	Random.init (int_of_float (Unix.time ()));
	let test_array = Array.init 1024 (fun i -> Random.int 256) in
	Printf.printf "First value: %d\n" test_array.(0);

	let (out_val,out_bits) = Array.fold_left (fun so_far gnu -> add_char y_data bin so_far gnu) (0,0) test_array in
	write_char bin out_val;
	
(*	Printf.printf "Compressed: %S\n" (to_hex (string_of_huff_buff bin));*)
	
	
	let bout = huff_buff_of_string (string_of_huff_buff bin) in
	
	let dehuff_table = compute_dehuff_table y_data 8 in
	
	let out_array = Array.make (Array.length test_array) (-1) in

	let rec do_until_done parts a i = (
		if i < Array.length a then (
			let (value, gnu) = get_char bout parts dehuff_table in
			a.(i) <- value;
			do_until_done gnu a (succ i)
		)
	) in
	do_until_done (0,0) out_array 0;

	Printf.printf "EQUAL??? %B\n" (test_array = out_array);

 	()
);;



(*
Printf.printf "NET:\n";;
let _ = (
	let temp_out = open_out_bin "temp_out.txt" in
	let temp_in  = open_in_bin  "temp_in.txt"  in

(*
	send temp_out temp_in (tag_of_string "RSND") "";;
	send temp_out temp_in (tag_of_string "LETGO") "Wikipedia";;
	send temp_out temp_in (tag_of_string "RECV") "";;

	let (adler_ok, tag, contents) = recv_base temp_in;;
	Printf.printf "Got %S: %S (OK? %B)\n" (string_of_tag tag) contents adler_ok;;

	let (adler_ok, tag, contents) = recv_base temp_in;;
	Printf.printf "Got %S: %S (OK? %B)\n" (string_of_tag tag) contents adler_ok;;
*)

	ignore (recv temp_out temp_in);

	close_out temp_out;
	close_in  temp_in ;
);;

Printf.printf "BTREE:\n";;

let _ = (

	let a = Btree.create ~cmp:(fun (a,b) (c,d) -> if a = c then compare b d else compare a c) 3 in
	
	for i = 1 to 1000 do
		let r = Random.int 30 in
		Btree.add a (r,i) "";
	done;

	Btree.print_tree a (fun (r,i) -> Printf.sprintf "%d,%d" r i) (fun x -> "");
	
(*
	let a = Btree.create ~cmp:(fun a b -> compare b a) 3 in
	let b = Btree.create ~cmp:(fun a b -> compare b a) 5 in

	for i = 1 to 1000 do
		let r = Random.int 30 - 10 in
		Btree.add a r (string_of_int i);
		Btree.add b r (string_of_int i);
(*
		try (
			Btree.delete b (Random.int 30 - 10)
		) with
			Not_found -> ()
	done;
	
	Btree.print_tree a (fun r -> Printf.sprintf "%d" r) (fun x -> "'" ^ x ^ "'");
	Btree.print_tree b (fun r -> Printf.sprintf "%d" r) (fun x -> "'" ^ x ^ "'");
*)
*)
);;

Printf.printf "BUFFER2:\n";;

let _ = (
	let b = Buffer2.create 10 in
	Printf.printf "Orig: %S\n" (Buffer2.contents b);
	Buffer2.add_string b "12345";
	Printf.printf "Add:  %S\n" (Buffer2.contents b);
	Buffer2.add_char b '6';
	Printf.printf "Char: %S\n" (Buffer2.contents b);
	Buffer2.remove_first b 1;
	Printf.printf "-1:   %S\n" (Buffer2.contents b);
	Buffer2.remove_first b 1;
	Printf.printf "-1:   %S\n" (Buffer2.contents b);
	Buffer2.remove_last b 1;
	Printf.printf "-1l:  %S\n" (Buffer2.contents b);
	Buffer2.remove_last b 1;
	Printf.printf "-1l:  %S\n" (Buffer2.contents b);
	Buffer2.remove_last b 1;
	Printf.printf "-1l:  %S\n" (Buffer2.contents b);
	Buffer2.remove_last b 1;
	Printf.printf "-1l:  %S\n" (Buffer2.contents b);
	Buffer2.remove_first b 1;
	Printf.printf "-1:   %S\n" (Buffer2.contents b);
	Buffer2.remove_first b 1;
	Printf.printf "-1:   %S\n" (Buffer2.contents b);
);;
*)