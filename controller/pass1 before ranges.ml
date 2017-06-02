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

let rec run_first_pass o i dir first_stats compression_setting supported_compressions =

	print "11111111 FIRST PASS 11111111\n";

	let frame_array_mutex = Mutex.create () in
	let frame_array_condition = Condition.create () in
	let frame_array = Array.make i.i_num_frames No_frame in
	let options_line_ref = ref "" in
	let first_pass_start_time = Sys.time () in
	let first_pass_frames_done_ref = ref 0 in

(*	let stats_rx = Str.regexp "in:\\([0-9]+\\) out:\\([0-9]+\\) type:\\(.\\) \\(.*;\\)" in*)
	let options_rx = Str.regexp "#options: " in

	(* FRAME PRINTING *)
	let print_frames_unsafe width = (
		let char_of_frame = function
			| Frame_I _ -> 'I'
			| Frame_notI _ -> 'i'
			| No_frame -> '.'
			| Frame_working -> '#'
			| Frame_redo (n,_) -> [| '0';'1';'2';'3';'4';'5';'6';'7';'8';'9';'A';'B';'C';'D';'E';'F' |].(min n 15)
		in
		Mutex.lock pm;
		for line = 0 to (i.i_num_frames - 1) / width do
			let from_frame = width * line in
			let to_frame = min (width * line + width - 1) (i.i_num_frames - 1) in
			let num_frames = to_frame - from_frame + 1 in
			let new_string = String.create num_frames in
			for offset = 0 to num_frames - 1 do
				new_string.[offset] <- char_of_frame frame_array.(from_frame + offset)
			done;
			print ~screen:false ~lock:false ~time:false "%s\n" new_string
		done;
		Mutex.unlock pm;
	) in

	(*******************************************)
	(* READ THE FRAMES WHICH WERE ALREADY MADE *)
	(*******************************************)
	let (out_stats_handle, in_name, out_name) = (
		let frametype_rx = Str.regexp "in:[0-9]+ out:\\([0-9]+\\) type:\\(.\\)" in
		let deleted_rx = Str.regexp "#DELETE \\([0-9]+\\)" in
		let version_rx = Str.regexp "#VERSION \\([0-9]+\\)" in
		(* Have 2 working stats files; one for reading and one for writing. This prevents any accidental screwing up when the program exits uncleanly *)
		(* The program will pick the largest file as the reading file *)
		let working_stats_name_1 = Filename.concat dir "working_stats1.txt" in
		let working_stats_name_2 = Filename.concat dir "working_stats2.txt" in

		(* Make the files, if they were not already made *)
		if is_something working_stats_name_1 then (
			if not (is_file working_stats_name_1) then (
				failwith (Printf.sprintf "ERROR: Working stats file '%s' is not a regular file\n" working_stats_name_1)
			)
		) else (
			let a = open_out working_stats_name_1 in
			close_out a
		);
		if is_something working_stats_name_2 then (
			if not (is_file working_stats_name_2) then (
				failwith (Printf.sprintf "ERROR: Working stats file '%s' is not a regular file\n" working_stats_name_2)
			)
		) else (
			let a = open_out working_stats_name_2 in
			close_out a
		);

		let (in_stats_handle, out_stats_handle, out_version, in_name, out_name) = (
			let handle1 = open_in working_stats_name_1 in
			let handle2 = open_in working_stats_name_2 in
			let v1_ref = ref None in
			let v2_ref = ref None in

			let rec test_line handle = (
				let line = input_line handle in
				if Str.string_match version_rx line 0 then (
					int_of_string (Str.matched_group 1 line)
				) else (
					test_line handle
				)
			) in

			(try
				v1_ref := Some (test_line handle1)
			with
				End_of_file -> ()
			);
			(try
				v2_ref := Some (test_line handle2)
			with
				End_of_file -> ()
			);
			close_in handle1;
			close_in handle2;
			(match (!v1_ref, !v2_ref) with
				| (None, None) when ((Unix.LargeFile.stat working_stats_name_1).Unix.LargeFile.st_size > (Unix.LargeFile.stat working_stats_name_2).Unix.LargeFile.st_size) -> (open_in working_stats_name_1, open_out working_stats_name_2, 1, working_stats_name_1, working_stats_name_2)
				| (None, None) -> (open_in working_stats_name_2, open_out working_stats_name_1, 1, working_stats_name_2, working_stats_name_1)
				| (Some x, None) -> print "#1 has version %d\n" x; (open_in working_stats_name_1, open_out working_stats_name_2, x + 1, working_stats_name_1, working_stats_name_2)
				| (None, Some y) -> print "#2 has version %d\n" y; (open_in working_stats_name_2, open_out working_stats_name_1, y + 1, working_stats_name_2, working_stats_name_1)
				| (Some x, Some y) when x > y -> print "#1 %d > #2 %d (read #1)\n" x y; (open_in working_stats_name_1, open_out working_stats_name_2, x + 1, working_stats_name_1, working_stats_name_2)
				| (Some x, Some y) -> print "#1 %d < #2 %d (read #2)\n" x y; (open_in working_stats_name_2, open_out working_stats_name_1, y + 1, working_stats_name_2, working_stats_name_1)
			)
		) in

		(try while true do
			let line = input_line in_stats_handle in
			let frame_ok = Str.string_match frametype_rx line 0 in
			if frame_ok then (
				let in_frame = int_of_string (Str.matched_group 1 line) in
				let frame_type = Str.matched_group 2 line in
				match frame_type with
				| "I" -> frame_array.(in_frame) <- Frame_I line
				|  _  -> frame_array.(in_frame) <- Frame_notI line
			) else (
				(* See if the frame is deleted *)
				let delete_ok = Str.string_match deleted_rx line 0 in
				if delete_ok then (
					let delete_this_frame = int_of_string (Str.matched_group 1 line) in
					frame_array.(delete_this_frame) <- No_frame
				) else (
					let options_ok = Str.string_match options_rx line 0 in
					if options_ok then (
						options_line_ref := line
					) (* Otherwise just give up *)
				)
			)
		done with End_of_file -> ());
		close_in in_stats_handle;

		(* Output all the frames which have already been done to the output frame file *)
		output_string out_stats_handle !options_line_ref; output_string out_stats_handle "\n";
		Array.iter (function
			| No_frame -> ()
			| Frame_working -> () (* This shouldn't be here now... *)
			| Frame_redo _ -> () (* This may be here... when I add support for it in the "#DELETE" line *)
			| Frame_I x -> (output_string out_stats_handle x; output_string out_stats_handle "\n")
			| Frame_notI x -> (output_string out_stats_handle x; output_string out_stats_handle "\n")
		) frame_array;
		(* Output the version *)
		output_string out_stats_handle ("#VERSION " ^ string_of_int out_version ^ "\n");
		flush out_stats_handle;

		(out_stats_handle, in_name, out_name)
	) in

	(* Ranger variables *)
	let splitter_list_mutex = Mutex.create () in
	let splitter_list_condition = Condition.create () in
	let splitter_list = List2.create () in
	let splitter_done_ref = ref false in

	(*********************)
	(* RANGER / SPLITTER *)
	(*********************)
	let rec check_nth_frame start_frame end_frame prev_frame_guts total_diff_so_far max_frame_diff max_frame_num current_frame_offset = (
		if current_frame_offset > o.o_split_frame_length || start_frame + current_frame_offset > end_frame then (
			start_frame + current_frame_offset
		) else (
			let current_threshold = (o.o_split_thresh -. 1.0) *. float_of_int (current_frame_offset - 1) /. float_of_int (1 - o.o_split_frame_length) +. o.o_split_thresh in
			
			let now_frame_guts = i.i_get_fast_frame_y (start_frame + current_frame_offset) in
			let diff_now = i.i_fast_frame_diff prev_frame_guts now_frame_guts in
			
			let new_total_diff = total_diff_so_far +. (float_of_int diff_now) in (* floats are ~2x faster than int64s on Win32. (int64s are ~50% faster on 64-bit Linux with my Athlon 64) *)
			let new_average_diff = new_total_diff /. float_of_int current_frame_offset in


			let (now_max_diff, now_max_num) = if diff_now > max_frame_diff
				then (diff_now, current_frame_offset)
				else (max_frame_diff, max_frame_num)
			in

(*			let current_multiple = float_of_int diff_now /. new_average_diff in*)
(*print " %-3d frame %3d\n  Current thresh %5.2f\n  Current diff %d\n  Avg diff %f\n  Max diff %d@%d) (or %d@%d)\n" (start_frame + current_frame_offset) current_frame_offset current_threshold diff_now new_average_diff max_frame_diff max_frame_num now_max_diff now_max_num;*)
			
			let max_multiple = float_of_int now_max_diff /. new_average_diff in
			
			if max_multiple >= current_threshold || current_threshold < 1.0 then (
				start_frame + now_max_num
			) else (
				check_nth_frame start_frame end_frame now_frame_guts new_total_diff now_max_diff now_max_num (succ current_frame_offset)
			)
		)
	) in



	let ranger_splitter_guts () = (
		let print_mutex = (fun a -> print ~screen:false a) in
		let print = (fun a -> print a) in
		let print_log = (fun a -> Types.print ~screen:false a) in
		let ranger_pass_ref = ref 1 in

		print "Ranger/splitter starting\n";

		let find_next_range from = (
			(*
				Big olde regular-expression-style parser
					a: Finds the next Frame_I
					b: Finds a No_frame and not a Frame_working, while updating the current I if another was found
					c: Finds another Frame_I after the No_frame, but not a Frame_working
					d: Finds another No_frame, in case the range can be made larger. Return if a Frame_I or Frame_working is found first
					e: Finds another Frame_I. Adds the GOP to the end if found, or sticks with the current range if a Frame_working
				I have a nice-looking flowchart of this; it makes it look much more reasonable...
				I hacked "w" into all the functions later; it's just a variable that keeps track of whether a Frame_working was encountered
				Note that this function treats Frame_redo as No_frame. The next function will look through the range and see what the largest Frame_redo is
			*)
			let last = pred i.i_num_frames in
			let q = false in
			
			if q then (
				Mutex.lock pm
			);
			let p = (fun a -> Types.print ~lock:false ~time:false a) in
(*
			let rec a on w = (if on > last then (if w then Range_working else Range_none) else match frame_array.(on) with
				| Frame_I    _  -> (if q then print "ab1\n"; b (succ on) w on)
				| Frame_notI _  -> (if q then print "aa2\n"; a (succ on) w   )
				| Frame_working -> (if q then print "aa3\n"; a (succ on) true)
				| No_frame      -> (if q then print "aa4\n"; a (succ on) w   )
				| Frame_redo _  -> (if q then print "aa5\n"; a (succ on) w   )
			)
			and b on w x = (if on > last then (if w then Range_working else Range_none) else match frame_array.(on) with
				| Frame_I    _  -> (if q then print "bb1\n"; b (succ on) w on)
				| Frame_notI _  -> (if q then print "bb2\n"; b (succ on) w x )
				| Frame_working -> (if q then print "ba3\n"; a (succ on) true)
				| No_frame      -> (if q then print "bc4\n"; c (succ on) w x )
				| Frame_redo _  -> (if q then print "bc5\n"; c (succ on) w x )
			)
			and c on w x = (if on > last then Range_found (x,last) else match frame_array.(on) with
				| Frame_I    _  -> (if q then print "cd1\n"; d (succ on) w x on)
				| Frame_notI _  -> (if q then print "cc2\n"; c (succ on) w x)
				| Frame_working -> (if q then print "ca3\n"; a (succ on) true)
				| No_frame      -> (if q then print "cc4\n"; c (succ on) w x)
				| Frame_redo _  -> (if q then print "cc5\n"; c (succ on) w x)
			)
			and d on w x y = (if on > last then Range_found (x,last) else match frame_array.(on) with
				| Frame_I    _  -> (if q then print "d!1\n"; Range_found (x,y))
				| Frame_notI _  -> (if q then print "dd2\n"; d (succ on) w x y)
				| Frame_working -> (if q then print "d!3\n"; Range_found (x,y))
				| No_frame      -> (if q then print "de4\n"; e (succ on) w x y)
				| Frame_redo _  -> (if q then print "de5\n"; e (succ on) w x y)
			)
			and e on w x y = (if on > last then Range_found (x,last) else match frame_array.(on) with
				| Frame_I    _  -> (if q then print "ed1\n"; d (succ on) w x on)
				| Frame_notI _  -> (if q then print "ee2\n"; e (succ on) w x y)
				| Frame_working -> (if q then print "e!3\n"; Range_found (x,y))
				| No_frame      -> (if q then print "ee4\n"; e (succ on) w x y)
				| Frame_redo _  -> (if q then print "ee5\n"; e (succ on) w x y)
			)
(*
				| Frame_I    _  -> (print "\n"; )
				| Frame_notI _  -> (print "\n"; )
				| No_frame      -> (print "\n"; )
				| Frame_working -> (print "\n"; )
*)
			in
*)

			let comp x y = min x y in

			let rec a on w = (if on > last then (if w then Range_working else Range_none) else match frame_array.(on) with
				| Frame_I     _ -> (if q then p "a1b "; b (succ on) w on)
				| Frame_notI  _ -> (if q then p "a2a "; a (succ on) w   )
				| Frame_working -> (if q then p "a3a "; a (succ on) true)
				| No_frame      -> (if q then p "a4a "; a (succ on) w   )
				| Frame_redo  _ -> (if q then p "a5a "; a (succ on) w   )
			)
			and b on w x = (if on > last then (if w then Range_working else Range_none) else match frame_array.(on) with
				| Frame_I        _ -> (if q then p "b1b "; b (succ on) w on)
				| Frame_notI     _ -> (if q then p "b2b "; b (succ on) w x)
				| Frame_working    -> (if q then p "b3a "; a (succ on) true)
				| No_frame         -> (if q then p "b4c "; c (succ on) w x)
				| Frame_redo (s,_) -> (if q then p "b5f "; f (succ on) w x s)
			)
			and c on w x = (if on > last then Range_found (x,last,0) else match frame_array.(on) with
				| Frame_I     _ -> (if q then p "c1d "; d (succ on) w x on)
				| Frame_notI  _ -> (if q then p "c2c "; c (succ on) w x)
				| Frame_working -> (if q then p "c3a "; a (succ on) true)
				| No_frame      -> (if q then p "c4c "; c (succ on) w x)
				| Frame_redo  _ -> (if q then p "c5c "; c (succ on) w x)
			)
			and d on w x y = (if on > last then Range_found (x,last,0) else match frame_array.(on) with
				| Frame_I    _  -> (if q then p "d1\n"; Range_found (x,y,0))
				| Frame_notI _  -> (if q then p "d2d "; d (succ on) w x y)
				| Frame_working -> (if q then p "d3\n"; Range_found (x,y,0))
				| No_frame      -> (if q then p "d4e "; e (succ on) w x y)
				| Frame_redo _  -> (if q then p "d5e "; e (succ on) w x y)
			)
			and e on w x y = (if on > last then Range_found (x,last,0) else match frame_array.(on) with
				| Frame_I    _  -> (if q then p "e1d "; d (succ on) w x on)
				| Frame_notI _  -> (if q then p "e2e "; e (succ on) w x y)
				| Frame_working -> (if q then p "e3!\n"; Range_found (x,y,0))
				| No_frame      -> (if q then p "e4e "; e (succ on) w x y)
				| Frame_redo _  -> (if q then p "e5e "; e (succ on) w x y)
			)
			and f on w x r = (if on > last then Range_found (x,last,r) else match frame_array.(on) with
				| Frame_I        _ -> (if q then p "f1g "; g (succ on) w x on r)
				| Frame_notI     _ -> (if q then p "f2f "; f (succ on) w x r)
				| Frame_working    -> (if q then p "f3a "; a (succ on) true)
				| No_frame         -> (if q then p "f4c "; c (succ on) w x)
				| Frame_redo (s,_) -> (if q then p "f5f "; f (succ on) w x (comp r s))
			)
			and g on w x y r = (if on > last then Range_found (x,last,r) else match frame_array.(on) with
				| Frame_I        _ -> (if q then p "g1\n"; Range_found (x,y,r))
				| Frame_notI     _ -> (if q then p "g2g "; g (succ on) w x y r)
				| Frame_working    -> (if q then p "g3\n"; Range_found (x,y,r))
				| No_frame         -> (if q then p "g4e "; e (succ on) w x y)
				| Frame_redo (s,_) -> (if q then p "g5h "; h (succ on) w x y (comp r s))
			)
			and h on w x y r = (if on > last then Range_found (x,last,r) else match frame_array.(on) with
				| Frame_I        _ -> (if q then p "h1g "; g (succ on) w x on r)
				| Frame_notI     _ -> (if q then p "h2h "; h (succ on) w x y r)
				| Frame_working    -> (if q then p "h3\n"; Range_found (x,y,r))
				| No_frame         -> (if q then p "h4e "; e (succ on) w x y)
				| Frame_redo (s,_) -> (if q then p "h5h "; h (succ on) w x y (comp r s))
			)
			in
			
			if q then (
				Mutex.unlock pm
			);


(*
				| Frame_I        _ -> (if q then print "1\n";  (succ on) )
				| Frame_notI     _ -> (if q then print "2\n";  (succ on) )
				| Frame_working    -> (if q then print "3\n";  (succ on) )
				| No_frame         -> (if q then print "4\n";  (succ on) )
				| Frame_redo (s,_) -> (if q then print "5\n";  (succ on) )
*)

			if from = 0 then (
				(* It's the first part of the array; count it as an I frame as long as it's not a Frame_working *)
				match frame_array.(0) with
				| Frame_working -> a 1 true
				| _ -> b 1 false 0
			) else (
				(* Normally *)
				a from false
			)
		) in

		(* Adds a split range to the list, and sets all the frames in that split to Frame_working (I think I need to do it here) *)
		let add_split (a,b,r) = (
			Mutex.lock splitter_list_mutex;
			print_mutex "> RANGER WAITING ON SPLITTER_LIST_CONDITION\n";
			while List2.length splitter_list >= Array.length o.o_agents_1 do
				print "Split list long enough; splitter spinning\n";
				Condition.wait splitter_list_condition splitter_list_mutex
			done;
			print_mutex "> RANGER GOING ON SPLITTER_LIST_CONDITION\n";

			Mutex.lock frame_array_mutex;
			print_mutex "> RANGER LOCKED FRAME_ARRAY_MUTEX\n";
			print_log "Setting all frames from %d to %d to Frame_working\n" (a + 1) b;
			for i = a + 1 to b do
				frame_array.(i) <- Frame_working;
			done;
(* PRINT FRAME ARRAY *)
(*
let a_string = String.create (Array.length frame_array) in
Array.iteri (fun i x ->
	a_string.[i] <- (
		match x with
		| Frame_I _ -> 'I'
		| Frame_notI _ -> 'i'
		| No_frame -> '#'
		| Frame_working -> '.'
	)
) frame_array;
Types.print ~screen:false "%s\n" a_string;
*)
(*			print_frames_unsafe 200;*)
(* !PRINT FRAME ARRAY *)
			print_mutex "> RANGER UNLOCKED FRAME_ARRAY_MUTEX\n";
			Mutex.unlock frame_array_mutex;
			List2.append splitter_list (a,b,r);
			Mutex.unlock splitter_list_mutex;
			
			print_mutex "> RANGER BROADCASTING SPLITTER_LIST_CONDITION\n";
			Condition.broadcast splitter_list_condition;
		) in


		(* Gets all the ranges, then does it again! *)
		let rec do_range from found_type = (
			(* found_type is:
					Range_found (x,y) if a range was found (x and y don't matter, but I'll set them to the last range endpoints)
					Range_working if no ranges were found, but a Frame_working was found
					Range_none if no ranges or Frame_working were found
			*)

			print "Ranges found: %s\n" (match found_type with | Range_found (x,y,r) -> "FOUND!" | Range_working -> "working..." | Range_none -> "NONE!");
			
			Mutex.lock frame_array_mutex;
			print_mutex "> RANGER LOCKED FRAME_ARRAY_MUTEX\n";
			let range_option = find_next_range from in
			print_mutex "> RANGER UNLOCKED FRAME_ARRAY_MUTEX\n";
			Mutex.unlock frame_array_mutex;
			
			match (found_type, range_option) with
			| (_, Range_found (a,b,r)) -> (
				
				print "Splitting range (%d,%d)\n" a b;
				let rec split_range go_from go_to = if go_to - go_from <= o.o_batch_length + o.o_split_frame_length then (
					(* It's small enough to add to the split list intact *)
					print "Adding all of (%d,%d,%d) to splitter_list\n" go_from go_to r;
					add_split (go_from,go_to,r);
					Range_found (go_from,go_to,r)
				) else (
					(* Subdivide *)
					let start_frame = go_from + o.o_batch_length in
					let best_new_i_frame = check_nth_frame start_frame go_to (i.i_get_fast_frame_y start_frame) 0.0 0 1 1 in
					print "Looks like the best place to split is right before frame %d\n" best_new_i_frame;
					print "Adding (%d,%d,%d) to splitter_list\n" go_from best_new_i_frame r;
					add_split (go_from,best_new_i_frame,r);
					split_range best_new_i_frame go_to
				) in
				split_range a b
			)
			| (Range_found (x,y,r), _) -> (
				(* A range was not found this time, but there was a range found previously *)
				Range_found (x,y,r)
			)
			| (Range_working, _) | (_, Range_working) -> (
				(* A range was found, but it was blocked by a Frame_working. Note that the "_" cannot be a Range_found, as they were caught in the last 2 matches *)
				Range_working
			)
			| (Range_none, Range_none) -> (
				(* Only return none if no ranges were found either now or previously *)
				Range_none
			)
		) in

		(* Find all the ranges, then return the status *)
		let rec find_all_ranges pass = (
			print_log "Ranger finding all ranges (ranger pass %d)\n" !ranger_pass_ref;
			match do_range 0 Range_none with
			| Range_found (x,y,r) -> (
				(* Found a range, so keep going *)
				print_log "Ranger found ranges; keep ranging\n";
				find_all_ranges pass
			)
			| Range_working -> (
				(* No ranges, but everything was working. Wait until somebody finished *)
				print_log "Ranger found only working frames; spin\n";
				Mutex.lock frame_array_mutex;
				print_mutex "> RANGER WAITING ON FRAME_ARRAY_CONDITION\n";
				Condition.wait frame_array_condition frame_array_mutex;
				print_mutex "> RANGER GOING ON FRAME_ARRAY_CONDITION\n";
				Mutex.unlock frame_array_mutex;
				print "Restarting the ranger!\n";
				incr ranger_pass_ref;
				find_all_ranges (succ pass)
			)
			| Range_none -> (
				(* No ranges, and nothing to do! *)
				print "No more unfinished ranges; ranger died!\n";
				splitter_done_ref := true;
				Condition.broadcast splitter_list_condition;
			)
		) in
		
		find_all_ranges 0
	) in


let uncompressed_ref = ref 0L in
let compressed_ref = ref 0L in

	(**********)
	(* DISHER *)
	(**********)
	let disher_guts (name,(ip,pf,pt),n) = (
		let print = (fun a -> print ~name:(name ^ " " ^ string_of_int n ^ " ") a) in
		let print_log = (fun a -> Types.print ~screen:false ~name:(name ^ " " ^ string_of_int n ^ " ") a) in
		print "starting on %s (id %d)\n" ip (Thread.id (Thread.self ()));
		
		let check_for_more_jobs_unsafe () = (
			not (List2.is_empty splitter_list && !splitter_done_ref)
		) in
		let check_for_more_jobs () = (
			Mutex.lock splitter_list_mutex;
			let a = check_for_more_jobs_unsafe () in
			Mutex.unlock splitter_list_mutex;
			a
		) in
		
		let timeout = 5 in
		
		let rec attempt_connection port = (
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
			let send = send sock in
			let recv () = recv sock in
			if not (check_for_more_jobs ()) then (
				print "attempt_connect sees no jobs left; DEAD\n";
				()
			) else if port > pt then (Unix.close sock; Unix.sleep timeout; attempt_connection pf) else (
				try (
					Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string ip, port));
					send "HELO" "";

					(* Compression figurer *)
					send "SCOM" supported_compressions;
					let compress = (match recv () with
						| ("SCOM", x) -> (
							(* Got some compressions *)
							let rec find_match on = (
								if on >= String.length x then (
									(* No matches found? Default to no compression *)
									'\x00'
								) else (
									if String.contains supported_compressions x.[on] then (
										(* Found! *)
										x.[on]
									) else (
										find_match (succ on)
									)
								)
							) in
							let used_compression = if o.o_nocomp then '\x00' else find_match 0 in
							compression_setting used_compression
						)
						| x -> raise (unexpected_packet x)
					) in
					(* !Supported compression *)

					check_out_job sock send recv port compress
				) with
				| _ -> (Unix.close sock; attempt_connection (succ port))
			)


		) and check_out_job sock send recv port compress = (
			Mutex.lock splitter_list_mutex;
			let rec fetch_a_range () = if List2.is_empty splitter_list then (
				if !splitter_done_ref then (
					None
				) else (
					Condition.wait splitter_list_condition splitter_list_mutex;
					fetch_a_range ()
				)
			) else (
				Some (List2.take_first splitter_list)
			) in
			let a_range_perhaps = fetch_a_range () in
			Mutex.unlock splitter_list_mutex;
			
			match a_range_perhaps with
			| None -> (
				print "check_out_job sees no jobs left; disconnect\n";
				close_connection sock send recv port
			)
			| Some (a,b,r) -> (
				print "check_out_job got frames %d - %d (%d)\n" a b r;
				Condition.broadcast splitter_list_condition;
				process_job sock send recv port compress a b r
			)


		) and process_job sock send recv port compress a b r = (
			let first_frame = max 0 (a - o.o_preseek) in
			let num_frames = b - a + 1 in
			
			try (
				let zone_string = List.fold_left (fun so_far -> function
					| {zone_end   = zb} when zb < a -> so_far (* Zones are outside range *)
					| {zone_start = za} when za > b -> so_far
					| {zone_start = za; zone_end = zb; zone_type = Zone_bitrate zf} -> (Printf.sprintf "%s%d,%d,b=%f" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - a)) (min (b - a) (zb - a)) zf)
					| {zone_start = za; zone_end = zb; zone_type = Zone_q zq} -> (Printf.sprintf "%s%d,%d,q=%d" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - a)) (min (b - a) (zb - a)) (int_of_float zq))
				) "" o.o_zones in
				
				let favs_md5 = Digest.file o.o_input_first_avs_full in
				let favs_send = List.fold_left (fun so_far gnu -> if so_far = "" then gnu else so_far ^ "/" ^ gnu) "" (split_on_dirs o.o_input_first_avs_full) in
				send "FAVS" (favs_md5 ^ favs_send);
				print_log "sent full AVS info %s = \"%s\"\n" (to_hex favs_md5) favs_send;

				send "VNFO" ((packN i.i_res_x) ^ (packN i.i_res_y) ^ (packN i.i_fps_n) ^ (packN i.i_fps_d));
				print_log "sent video info %dx%d @ %d/%d\n" i.i_res_x i.i_res_y i.i_fps_n i.i_fps_d;
				
				send "ZONE" zone_string;
				print_log "sent zone string %S\n" zone_string;

				send "RANG" ((packN (a + o.o_seek)) ^ (packN (b + o.o_seek)));
				print_log "sent range (%d,%d)\n" (a + o.o_seek) (b + o.o_seek);

				send "1PAS" (Printf.sprintf "%s --qcomp %F --cplxblur %F --qblur %F --qpmin %d --qpmax %d --qpstep %d --ipratio %F --pbratio %F" o.o_first o.o_rc_qcomp o.o_rc_cplxblur o.o_rc_qblur o.o_rc_qpmin o.o_rc_qpmax o.o_rc_qpstep o.o_rc_ipratio o.o_rc_pbratio);

(*
					let rec send_bytes = (
						let max_64 = Int64.of_int max_string_length in
						fun num_64 -> (
							if num_64 <= max_64 then (
								let num = Int64.to_int num_64 in
								let str = String.create num in
								really_input get str 0 num;
								send "FRAM" str;
							) else (
								let str = String.create max_string_length in
								really_input get str 0 max_string_length;
								send "FRAM" str;
								send_bytes (Int64.sub num_64 max_64)
							);
						)
					) in
					send_bytes total_bytes_64;
*)

				(match recv () with
					| ("ENCD","agent") -> (
						(* Don't need to do anything; just wait for the agent to send everything back *)
						print "agent-based encoding; waiting for agent to finish\n";

						(*
							This part is here just so execution stops here instead of after the frames are set to No_frame.
							If the splitter gets ahold of frame_array after the frames have been set to No_frame, it will
							assume that the frames are actually not rendered, and will add them to the next range.
							This next line does not eliminate that possibility, unfortunately; it just minimizes it
						*)
						match recv () with
						| ("DONE","") -> ()
						| x -> raise (unexpected_packet x)
					)
					| ("ENCD","controller") -> (
	
						let total_bytes_64 = Int64.mul (Int64.of_int num_frames) (Int64.of_int i.i_bytes_per_frame) in
						let get = Unix.open_process_in (Printf.sprintf "\"\"%s\" -raw -seek %d -frames %d -o - \"%s\" 2>%s\"" i.i_avs2yuv (first_frame + o.o_seek) (b - first_frame + 1) o.o_input_avs dev_null) in
						if first_frame <> a then (
							let ignore_bytes = i.i_bytes_per_frame * (a - first_frame) in
							let str = String.create ignore_bytes in
							really_input get str 0 ignore_bytes
						);
			
(* THREAD STRING *)
(*
						(try
							let rec send_bytes = (
								let max_64 = Int64.of_int max_string_length in
								fun num_64 -> (
									if num_64 <= max_64 then (
										let num = Int64.to_int num_64 in
										really_input get thread_string 8 num;
										send_full sock ~len:num thread_string;
										String.blit max_string_length_bin 0 thread_string 4 4 (* Reset the length! *)
									) else (
										really_input get thread_string 8 max_string_length;
										send_full sock thread_string;
										send_bytes (Int64.sub num_64 max_64)
									)
								)
							) in
							send_bytes total_bytes_64;
						with
							x -> (ignore (Unix.close_process_in get); raise x)
						);
*)

(* COMPRESSION *)
						compress num_frames get sock;

						ignore (Unix.close_process_in get);
				
						send "EOF!" "";
					)
					| x -> raise (unexpected_packet x)
				);

				(* Set all the frames in the range to No_frame, just in case the stats file is not completely written and some Frame_workings are left over. *)
				Mutex.lock frame_array_mutex;
				for n = a + 1 to b do
					frame_array.(n) <- No_frame
				done;
				Mutex.unlock frame_array_mutex;
				
				(* Receive output *)
				let rec recv_stats () = (
					match recv () with
					| ("STAT", x) -> (
						(* A stat line *)
						match stats_line_of_string x with
						| Some s -> (
							(* It's a frame! *)
							s.stats_in <- s.stats_in + a;
							s.stats_out <- s.stats_out + a;
							
							if s.stats_out = b && s.stats_type <> Stats_frame_I && b <> i.i_num_frames - 1 then (
								(* The last frame was NOT an I frame, so delete it from the thingie *)
								(* But only if it's not the last frame in the file *)
								print "deleting frame %d\n" s.stats_out;
								Printf.fprintf out_stats_handle "#DELETE %d\n" s.stats_out;
								flush out_stats_handle;
								frame_array.(s.stats_out) <- No_frame;
								recv_stats ()
							) else if s.stats_in = a && a <> 0 then (
								(* First frame of the bunch; ignore *)
								recv_stats ()
							) else (
								Printf.fprintf out_stats_handle "%s\n" (string_of_stats_line s);
								flush out_stats_handle;
								frame_array.(s.stats_out) <- (if s.stats_type = Stats_frame_I then Frame_I (string_of_stats_line s) else Frame_notI (string_of_stats_line s));
								recv_stats ()
							)
						)
						| None -> (
							if Str.string_match options_rx x 0 then (
								(* It's an option line! *)
								print_log "got stats option line\n";
								if !options_line_ref = "" then (
									Printf.fprintf out_stats_handle "%s\n" x;
									options_line_ref := x;
								);
								recv_stats ()
							) else (
								print "WARNING: Don't know how to handle %S\n" x
							)
						)
					)
					| ("EOF!","") -> (print_log "done recieving stats\n")
					| x -> raise (unexpected_packet x)
				) in

				
				let reset_range () = (
					for frame = (if a = 0 then a else a + 1) to b do
						if frame_array.(frame) = Frame_working then (
							print "frame number %d was still set to working; reset to none\n" frame;
							frame_array.(frame) <- No_frame
						)
					done;
				) in

				(* Receive the stats and reset the frame types if they are incorrect *)
				print_log "receiving stats\n";
				Mutex.lock frame_array_mutex;
				(try
					recv_stats ();
				with
					x -> (reset_range (); Mutex.unlock frame_array_mutex; raise x)
				);
				reset_range ();
				Mutex.unlock frame_array_mutex;
						

				
				(* Print the FPS *)
				Mutex.lock frame_array_mutex;
				first_pass_frames_done_ref := !first_pass_frames_done_ref + b - a + (if a = 0 then 1 else 0); (* Ignore the first frame in the job, except for the first frame in the video *)
				Types.print "Done with %d frames, at %f fps total\n" !first_pass_frames_done_ref (float_of_int !first_pass_frames_done_ref /. (Sys.time () -. first_pass_start_time));
				Mutex.unlock frame_array_mutex;

				Condition.broadcast frame_array_condition;

				check_out_job sock send recv port compress
			) with
			| x -> (
				(match x with
					| Unix.Unix_error (x,y,z) -> print "process_job failed with (%S,%s,%s); putting job (%d,%d) back\n" (Unix.error_message x) y z a b
					| x -> print "process_job failed with %S; putting job (%d,%d) back\n" (Printexc.to_string x) a b
				);
				
(*				ignore (Unix.close_process_in get);*)
				
				Mutex.lock splitter_list_mutex;
				List2.append splitter_list (a,b,r);
				Mutex.unlock splitter_list_mutex;
				Condition.broadcast splitter_list_condition;
				close_connection sock send recv port
			)


		) and close_connection sock send recv port = (
			print "close connection\n";
(*			Unix.shutdown sock Unix.SHUTDOWN_ALL;*)
			Unix.close sock;
			attempt_connection port
		) in
		
		attempt_connection pf;
		print "exiting...\n";
	) in

	let dishing_threads = Array.map (fun x -> Thread.create disher_guts x) o.o_agents_1 in

	let splitter_thread = Thread.create ranger_splitter_guts () in
	Thread.join splitter_thread;
	print "Ranger kicked the bucket\n";

	Array.iter (fun x -> Thread.join x) dishing_threads;
	print "Dishers kicked the bucket\n";
	
	print "Closing files and cleaning up directory\n";
	(* Output all the frames to the first_stats file *)
	(try
		let options_ok = Str.string_match options_rx !options_line_ref 0 in
		if options_ok then (
			(* Options line OK! *)
			let final_stats = open_out first_stats in
			Printf.fprintf final_stats "%s\n" !options_line_ref;
			let rec write_stats index = (
				if index < Array.length frame_array then (
					(* Keep going... *)
					match frame_array.(index) with
					| Frame_I x | Frame_notI x | Frame_redo (_,Some x) -> (
						Printf.fprintf final_stats "%s\n" x;
						write_stats (succ index)
					)
					| No_frame -> (
						(* This shouldn't happen *)
						print "WARNING: Everything's done but there are still uncompleted frames; re-running first pass\n";
						close_out final_stats;
						close_out out_stats_handle;
						run_first_pass o i dir first_stats compression_setting supported_compressions
					)
					| Frame_working -> (
						(* Nor should this *)
						print "WARNING: Everything's done but there are still frames marked as being worked on; re-running first pass\n";
						close_out final_stats;
						close_out out_stats_handle;
						run_first_pass o i dir first_stats compression_setting supported_compressions
					)
					| Frame_redo (_,None) -> (
						(* This is bad because there is no info on what frame is supposed to be here *)
						print "WARNING: Everything's done but there are still uncompleted frames; re-running first pass\n";
						close_out final_stats;
						close_out out_stats_handle;
						run_first_pass o i dir first_stats compression_setting supported_compressions
					)
				) else (
					(* Done! *)
					close_out final_stats;
					close_out out_stats_handle;
					
					(* Make the silly assumption that there are only 3 files in the dir. STOP PUTTING RANDOM FILES IN THE TEMP DIR. *)
					Sys.remove in_name;
					Sys.remove out_name;
					(try Sys.remove (Filename.concat dir "settings.txt") with _ -> ()); (* The settings file may not exist *)

					(* Now delete the dir *)
					Unix.rmdir dir;
					(* THIS IS WHERE THE FUNCTION RETURNS NORMALLY! *)
				)
			) in
			write_stats 0;
			print "FIRST PASS DONE in %f seconds\n" (Sys.time () -. first_pass_start_time) (*(i.i_num_frames /. (Sys.time () -. first_pass_start_time))*);
		) else (
			(* Options line NOT OK! *)
			print "WARNING: Options line not OK; re-rendering the first GOP to get it\n";
			output_string out_stats_handle "#DELETE 0\n";
			close_out out_stats_handle;
			run_first_pass o i dir first_stats compression_setting supported_compressions
		)
	with
	| x -> print "ERROR: output file probably not written (%S)\n" (Printexc.to_string x)
	)
;;
(* LE END *)
