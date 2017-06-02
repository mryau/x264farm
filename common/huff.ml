open Types;;

type huff_buff_t = {
	huff_string : string;
	huff_string_length : int;
	mutable huff_pos : int;
};;
exception Buffer_full;;
exception Buffer_empty;;

let huff_buff_of_string ?(start=0) s = {huff_string = s; huff_string_length = String.length s; huff_pos = min (String.length s) start};;
let new_huff_buff len = {huff_string = String.create len; huff_string_length = len; huff_pos = 0};;

let write_char b c =
	if b.huff_pos >= b.huff_string_length then (
		raise Buffer_full
	) else (
		b.huff_string.[b.huff_pos] <- Char.chr c;
		b.huff_pos <- b.huff_pos + 1;
	)
;;

let read_char b =
	if b.huff_pos >= b.huff_string_length then (
		raise Buffer_empty
	) else (
		let c = Char.code b.huff_string.[b.huff_pos] in
		b.huff_pos <- b.huff_pos + 1;
		c
	)
;;

let is_full a = a.huff_pos >= a.huff_string_length;;

let string_of_huff_buff a = String.sub a.huff_string 0 a.huff_pos;;
let innards_of_huff_buff a = (a.huff_string, a.huff_pos);;

let set_huff_buff_pos a b = a.huff_pos <- b;;

let add_char_precomputed a b (in_incomplete_byte,in_incomplete_bits) c = (* a=array, b=buffer, c=character. I didn't mean to make it look like I'm making up variable names *)
(*	Printf.printf "Adding byte %d to buffer with (%d,%d)\n" c in_incomplete_byte in_incomplete_bits;*)
	let (bytes,out_incomplete_bits) = a.(in_incomplete_bits).(c) in
	let byte_array_length = Array.length bytes in
	if byte_array_length = 1 then (
		if out_incomplete_bits < 8 then (
			(* This indicates that the current input byte is still not completed *)
(*			Printf.printf " Current byte still not completed; return (%d,%d)\n" (in_incomplete_byte lor bytes.(0)) out_incomplete_bits;*)
			(in_incomplete_byte lor bytes.(0), out_incomplete_bits)
		) else (
			(* The first byte is the only byte, and it has been completed *)
(*			Printf.printf " Current byte barely done; output %d and return (0,0)\n" (in_incomplete_byte lor bytes.(0));*)
			write_char b (in_incomplete_byte lor bytes.(0));
			(0,0)
		)
	) else (
		(* The current input byte is done; output *)
(*		Printf.printf " First byte done; output %d\n" (in_incomplete_byte lor bytes.(0));*)
		write_char b (in_incomplete_byte lor bytes.(0));
		
		(* Now output the middle bytes *)
		for q = 1 to byte_array_length - 2 do
(*			Printf.printf "  Output %d\n" bytes.(q);*)
			write_char b bytes.(q);
		done;
		
		(* Return the last byte *)
		if out_incomplete_bits = 8 then (
			(* This only works if byte_array_length > 1 *)
(*			Printf.printf " Last byte also done; output %d and return (0,0)\n" bytes.(byte_array_length - 1);*)
			write_char b bytes.(byte_array_length - 1);
			(0,0)
		) else (
(*			Printf.printf " Return (%d,%d)\n" (bytes.(byte_array_length - 1)) out_incomplete_bits;*)
			(bytes.(byte_array_length - 1), out_incomplete_bits)
		)
	)
;;



let add_char a b (in_incomplete_byte,in_incomplete_bits) c =
(*	Printf.printf "Adding byte %d to buffer with (%d,%d)\n" c in_incomplete_byte in_incomplete_bits;*)
	let (len,num) = a.(c) in
(*	let number_of_bytes = (in_incomplete_bits + len + 7) / 8 in*)
	
	let rec make_next_byte from_bit = (
		if from_bit = 7 then (
			(* The byte goes from 7 to 0 *)
			write_char b (num land 0xFF);
			(0,0)
		) else if from_bit > 7 then (
			(* A full byte; keep going *)
			write_char b (num lsr (from_bit - 7) land 0xFF);
			make_next_byte (from_bit - 8)
		) else (
			(* It's the end! *)
			let mask = [| 0b00000001; 0b00000011; 0b00000111; 0b00001111; 0b00011111; 0b00111111; 0b01111111 |].(from_bit) in
			((num land mask) lsl (7 - from_bit), from_bit + 1)
		)
	) in
	(* Make the first byte, then call the make_next_byte if needed *)
	let out = (
		let out_incomplete_bits = in_incomplete_bits + len in
		if out_incomplete_bits < 8 then (
			(* Don't even output a byte *)
			(* Don't bother with a mask, since len must be < 8 *)
			(in_incomplete_byte lor (num lsl (8 - out_incomplete_bits)), out_incomplete_bits)
		) else if out_incomplete_bits = 8 then (
			(* Output a byte but don't call make_next_byte *)
			write_char b (in_incomplete_byte lor num);
			(0,0)
		) else (
			(* Output the current byte and call make_next_byte *)
			let mask = [| 0b11111111; 0b01111111; 0b00111111; 0b00011111; 0b00001111; 0b00000111; 0b00000011; 0b00000001 |].(in_incomplete_bits) in
			write_char b (in_incomplete_byte lor ((num lsr (len - 8 + in_incomplete_bits)) land mask));
			make_next_byte (len - 9 + in_incomplete_bits)
		)
	) in
	out
;;


(*
for start_bits = 0 to 7 do
	Printf.printf "Bits = %d\n" start_bits;
	let now_array = y_use_array.(start_bits) in
	for byte = 0 to 255 do
		Printf.printf " Byte %d\n" byte;
		let (len,num) = yarray.(byte) in
		Printf.printf "  (%d,%d) = %s\n" len num (bin_of_int num len);
		let byte_array_length = (start_bits + len + 7) / 8 in
		Printf.printf "  Length %d\n" byte_array_length;
		let byte_array = Array.init byte_array_length (fun i ->
			let msb = len - 1 - ((~- start_bits) + (8 * i)) in
			let lsb = msb - 7 in
			let good_msb = min (len - 1) msb in
			let good_lsb = max 0 lsb in

			let bit_mask = [| 0b00000001; 0b00000011; 0b00000111; 0b00001111; 0b00011111; 0b00111111; 0b01111111; 0b11111111 |].(good_msb - good_lsb) in
			
			if good_lsb = 0 then (
				(num land bit_mask) lsl (~- lsb)
			) else (
				(num lsr lsb) land bit_mask
			)
		) in
		let used_bits = 8 - (8 * byte_array_length - start_bits - len) in
		now_array.(byte) <- (byte_array, used_bits);
	done;
done;;
*)

(**********)
(* DEHUFF *)
(**********)
(* WOW. I am so SMRT! *)
(* Takes data in the form [| (len,val); ... |] *)
(* This function returns (int * int * int) option3_t array, where the base types indicate (decoded_value, total_bits, current_level_used_bits) *)
let compute_dehuff_table data len =
	let array_length = 1 lsl len in
	let data_out = Array.make array_length Zero in

	let rec find_spot current_out value orig_bits bits index = (
		if bits <= len then (
			(* This is the level! *)
			let pad_bits = len - bits in
			let base = index lsl pad_bits in
			for add = 0 to (1 lsl pad_bits) - 1 do
				current_out.(base lor add) <- One (value, orig_bits, bits)
			done;
		) else (
			(* Sub-array *)
			let extra_bits = bits - len in
			let current_index = index lsr extra_bits in
			match current_out.(current_index) with
			| Zero -> (
				(* Make a new array *)
				let new_out = Array.make array_length Zero in
				let new_index = (index lxor (current_index lsl extra_bits)) in
				find_spot new_out value orig_bits extra_bits new_index;
				current_out.(current_index) <- Many new_out
			)
			| One x -> (
				failwith "Huffman table incorrect! Yell at whoever made it!"
			)
			| Many new_out -> (
				(* Add to an already-existing array *)
				let new_index = (index lxor (current_index lsl extra_bits)) in
				find_spot new_out value orig_bits extra_bits new_index;
			)
		)
	) in

	Array.iteri (fun value (bits, index) ->
		find_spot data_out value bits bits index
	) data;
	
	(len, data_out);
;;

let rec get_char b (part_byte, part_bits) (bits_per_level, tree) =
	if part_bits < bits_per_level then (
		if is_full b then (
			(* The buffer's full! Pad the last lookup and make sure it's consistant *)
(*			Printf.printf "Part bits < bits per level (%d < %d)\n" part_bits bits_per_level;*)
			let pad_bits = bits_per_level - part_bits in
			let mask = lnot ((-1) lsl pad_bits) in
			let index_from = part_byte lsl pad_bits in
			let index_to = index_from lor mask in
			match (tree.(index_from), tree.(index_to)) with
			| (One ((value, total_bits, used_bits) as x), One y) when x = y -> (
				let unused_bits = part_bits - used_bits in
				let new_part_byte = part_byte lxor ((part_byte lsr unused_bits) lsl unused_bits) in
				(value, (new_part_byte, unused_bits))
			)
			| _ -> (failwith "Last byte contains incomplete Huffman symbol")
		) else (
			let new_part_byte = (part_byte lsl 8) lor (read_char b) in
			get_char b (new_part_byte, part_bits + 8) (bits_per_level, tree)
		)
	) else (
		let index = part_byte lsr (part_bits - bits_per_level) in
		match tree.(index) with
		| One (value, total_bits, used_bits) -> (
			let unused_bits = part_bits - used_bits in
			let new_part_byte = part_byte lxor ((part_byte lsr unused_bits) lsl unused_bits) in
			(value, (new_part_byte, unused_bits))
		)
		| Many new_tree -> (
			let unused_bits = part_bits - bits_per_level in
			let new_part_byte = part_byte lxor ((part_byte lsr unused_bits) lsl unused_bits) in
			get_char b (new_part_byte, unused_bits) (bits_per_level, new_tree)
		)
		| Zero -> (
			failwith "Huffman table has an uncoded string!"
		)
	)
;;

let y_data = [| (1,1);(3,2);(4,2);(6,5);(7,7);(7,8);(8,9);(8,10);(9,11);(9,12);(9,13);(9,14);(10,13);(10,14);(10,15);(10,16);(11,15);(11,16);(11,17);(11,18);(11,19);(11,20);(12,16);(12,17);(12,18);(12,19);(12,20);(12,21);(12,22);(13,16);(13,17);(13,18);(13,19);(13,20);(13,21);(13,22);(13,23);(14,16);(14,17);(14,18);(14,19);(14,20);(14,21);(14,22);(14,23);(15,16);(15,17);(15,18);(15,19);(15,20);(15,21);(15,22);(15,23);(16,16);(16,17);(16,18);(16,19);(16,20);(16,21);(16,22);(16,23);(17,16);(17,17);(17,18);(17,19);(17,20);(17,21);(17,22);(17,23);(18,16);(18,17);(18,18);(18,19);(18,20);(18,21);(18,22);(18,23);(19,16);(19,17);(19,18);(19,19);(19,20);(19,21);(19,22);(19,23);(20,15);(20,16);(20,17);(20,18);(20,19);(20,20);(20,21);(20,22);(21,14);(21,15);(21,16);(21,17);(21,18);(21,19);(21,20);(21,21);(22,13);(22,14);(22,15);(22,16);(22,17);(22,18);(22,19);(22,20);(23,10);(23,11);(23,12);(23,13);(23,14);(23,15);(23,16);(23,17);(24,3);(24,4);(24,5);(24,6);(24,7);(24,8);(24,9);(24,10);(25,0);(25,1);(25,2);(25,3);(25,4);(25,5);(24,11);(24,12);(24,13);(24,14);(24,15);(24,16);(24,17);(24,18);(24,19);(23,18);(23,19);(23,20);(23,21);(23,22);(23,23);(23,24);(23,25);(22,21);(22,22);(22,23);(22,24);(22,25);(22,26);(22,27);(21,22);(21,23);(21,24);(21,25);(21,26);(21,27);(21,28);(21,29);(20,23);(20,24);(20,25);(20,26);(20,27);(20,28);(20,29);(20,30);(20,31);(19,24);(19,25);(19,26);(19,27);(19,28);(19,29);(19,30);(19,31);(18,24);(18,25);(18,26);(18,27);(18,28);(18,29);(18,30);(18,31);(17,24);(17,25);(17,26);(17,27);(17,28);(17,29);(17,30);(17,31);(16,24);(16,25);(16,26);(16,27);(16,28);(16,29);(16,30);(16,31);(15,24);(15,25);(15,26);(15,27);(15,28);(15,29);(15,30);(15,31);(14,24);(14,25);(14,26);(14,27);(14,28);(14,29);(14,30);(14,31);(13,24);(13,25);(13,26);(13,27);(13,28);(13,29);(13,30);(13,31);(12,23);(12,24);(12,25);(12,26);(12,27);(12,28);(12,29);(11,21);(11,22);(11,23);(11,24);(11,25);(10,17);(10,18);(10,19);(10,20);(10,21);(9,15);(9,16);(9,17);(8,11);(8,12);(8,13);(7,9);(6,6);(6,7);(4,3);(3,3) |];;
let u_data = [| (1,1);(3,1);(5,2);(6,2);(8,3);(8,4);(9,4);(10,4);(10,5);(11,5);(12,5);(12,6);(12,7);(13,6);(13,7);(14,6);(14,7);(14,8);(15,7);(15,8);(16,7);(16,8);(16,9);(16,10);(17,8);(17,9);(17,10);(18,8);(18,9);(18,10);(18,11);(19,8);(19,9);(19,10);(19,11);(20,8);(20,9);(20,10);(20,11);(21,9);(21,10);(21,11);(22,9);(22,10);(22,11);(22,12);(22,13);(23,10);(23,11);(23,12);(23,13);(24,11);(24,12);(24,13);(24,14);(25,12);(25,13);(25,14);(25,15);(25,16);(26,13);(26,14);(26,15);(26,16);(26,17);(26,18);(27,16);(27,17);(27,18);(27,19);(27,20);(28,18);(28,19);(28,20);(28,21);(28,22);(28,23);(28,24);(29,21);(29,22);(29,23);(29,24);(29,25);(29,26);(29,27);(30,23);(30,24);(30,25);(30,26);(30,27);(30,28);(30,29);(30,30);(30,31);(30,32);(31,21);(31,22);(31,23);(31,24);(31,25);(31,26);(31,27);(31,28);(31,29);(31,30);(31,31);(31,32);(32,0);(32,1);(32,2);(32,3);(32,4);(32,5);(32,6);(32,7);(32,8);(32,9);(32,10);(32,11);(32,12);(32,13);(32,14);(32,15);(32,16);(32,17);(32,18);(32,19);(32,20);(32,21);(32,22);(32,23);(32,24);(32,25);(32,26);(32,27);(32,28);(32,29);(32,30);(32,31);(32,32);(32,33);(32,34);(32,35);(32,36);(32,37);(32,38);(32,39);(32,40);(32,41);(31,33);(31,34);(31,35);(31,36);(31,37);(31,38);(31,39);(31,40);(31,41);(31,42);(31,43);(31,44);(31,45);(30,33);(30,34);(30,35);(30,36);(30,37);(30,38);(30,39);(30,40);(30,41);(29,28);(29,29);(29,30);(29,31);(29,32);(29,33);(29,34);(29,35);(28,25);(28,26);(28,27);(28,28);(28,29);(28,30);(28,31);(27,21);(27,22);(27,23);(27,24);(27,25);(26,19);(26,20);(26,21);(26,22);(26,23);(25,17);(25,18);(25,19);(25,20);(25,21);(24,15);(24,16);(24,17);(24,18);(24,19);(23,14);(23,15);(23,16);(23,17);(22,14);(22,15);(22,16);(22,17);(21,12);(21,13);(21,14);(21,15);(20,12);(20,13);(20,14);(20,15);(19,12);(19,13);(19,14);(19,15);(18,12);(18,13);(18,14);(18,15);(17,11);(17,12);(17,13);(16,11);(16,12);(16,13);(15,9);(15,10);(15,11);(14,9);(14,10);(14,11);(13,8);(13,9);(12,8);(12,9);(11,6);(11,7);(10,6);(10,7);(9,5);(8,5);(7,3);(6,3);(5,3);(2,1) |];;
