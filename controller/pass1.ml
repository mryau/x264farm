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
(*  *)

(* perl -e "$old = 'a'; while($new = <>) {$old eq $new ? () : print $new; $old = $new;}" < "1st pass style.txt" > "1st pass style normalized.txt" *)

open Types;;
open Controllerinclude;;
open Pack;;
open Net;;



let rec run_first_pass o i dir first_stats compression_setting supported_compressions =

	print "\n";
	print "FIRST PASS\n";

	let options_line_ref = ref "" in
	let first_pass_start_time = Sys.time () in
	let first_pass_frames_done_ref = ref 0 in

	let options_rx = Str.regexp "#options: " in

	(* This is important to have here since the batch length may be larger than the number of frames, which apparently screws everything up *)
(*	let first_batch = min (i.i_num_frames / Array.length o.o_agents_1) o.o_batch_length in*)
	let first_batch = min (i.i_num_frames / max 1 (Array.length o.o_agents)) (Array.fold_left (fun so_far {ac_agents = (x,_)} -> so_far + x) 0 o.o_agents) in
	let first_batch = min (i.i_num_frames / (max 1 (Array.fold_left (fun so_far {ac_agents = (x,_)} -> so_far + x) 0 o.o_agents))) o.o_batch_length in

	(*******************************************)
	(* READ THE FRAMES WHICH WERE ALREADY MADE *)
	(*******************************************)
	let (out_stats_handle, in_name, out_name, file_ranges) = (

		let frametype_rx   = Str.regexp "in:[0-9]+ out:\\([0-9]+\\) type:\\(.\\)" in
		let deleted_rx     = Str.regexp "#DELETE \\([0-9]+\\) \\([0-9]+\\)" in
		let deleted_old_rx = Str.regexp "#DELETE \\([0-9]+\\)" in
		let version_rx     = Str.regexp "#VERSION \\([0-9]+\\)" in
		let split_rx       = Str.regexp "#SPLIT \\([0-9]+\\) \\([0-9.]+\\)" in

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

		(* Now pick which one has the largest version and use it as the input file *)
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
				| (Some x, None) -> print ~screen:false "#1 has version %d\n" x; (open_in working_stats_name_1, open_out working_stats_name_2, x + 1, working_stats_name_1, working_stats_name_2)
				| (None, Some y) -> print ~screen:false "#2 has version %d\n" y; (open_in working_stats_name_2, open_out working_stats_name_1, y + 1, working_stats_name_2, working_stats_name_1)
				| (Some x, Some y) when x > y -> print ~screen:false "#1 %d > #2 %d (read #1)\n" x y; (open_in working_stats_name_1, open_out working_stats_name_2, x + 1, working_stats_name_1, working_stats_name_2)
				| (Some x, Some y) -> print ~screen:false "#1 %d < #2 %d (read #2)\n" x y; (open_in working_stats_name_2, open_out working_stats_name_1, y + 1, working_stats_name_2, working_stats_name_1)
			)
		) in

		(* Now put all of the frames in a big array *)
		let frame_array = Array.make i.i_num_frames No_frame in
		let split_list_ref = ref [] in (* A list of all the split points found in the file, of form (frame, thresh) :: [] *)
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
				let delete_range_ok = Str.string_match deleted_rx line 0 in
				if delete_range_ok then (
					let delete_from = int_of_string (Str.matched_group 1 line) in
					let delete_to   = int_of_string (Str.matched_group 2 line) in
					Array.fill frame_array delete_from (delete_to - delete_from + 1) No_frame
				) else (
					let deleted_frame_ok = Str.string_match deleted_old_rx line 0 in
					if deleted_frame_ok then (
						let deleted_frame = int_of_string (Str.matched_group 1 line) in
						frame_array.(deleted_frame) <- No_frame
					) else (
						let split_ok = Str.string_match split_rx line 0 in
						if split_ok then (
							let split_frame = int_of_string (Str.matched_group 1 line) in
							let split_thresh = float_of_string (Str.matched_group 2 line) in
							split_list_ref := (split_frame, split_thresh) :: !split_list_ref;
						) else (
							let options_ok = Str.string_match options_rx line 0 in
							if options_ok then (
								options_line_ref := line
							) (* else give up *)
						)
					)
				)
			)
		done with
			End_of_file -> ()
		);
		close_in in_stats_handle;
(*
Array.iteri (fun i -> function
	| No_frame     -> print ~screen:false "%8d NOFRAME\n" i
	| Frame_I x    -> print ~screen:false "%8d    I    %s\n" i x
	| Frame_notI x -> print ~screen:false "%8d notI    %s\n" i x
) frame_array;
*)
		let split_list = List.sort (fun (a,_) (b,_) -> compare a b) !split_list_ref in
(* DELETEME *)
(*
List.iter (fun (f,t) ->
	Printf.printf "#SPLIT %d = %f\n" f t;
) split_list;
exit 458;
*)
(* !DELETEME *)

		(* Do some preprocessing *)
		(* First, extend any No_frames to the next I frame *)
		(
			let rec extend_no_frame delete f = if f = i.i_num_frames then () else (
				match frame_array.(f) with
				| No_frame     -> extend_no_frame true (succ f)
				| Frame_I _    -> extend_no_frame false (succ f)
				| Frame_notI _ -> (
					if delete then frame_array.(f) <- No_frame;
					extend_no_frame delete (succ f)
				)
			) in
			extend_no_frame false 0
		);
		(* Now extend the No_frames the other way *)
		(
			let rec extend_no_frame delete f = if f = 0 then () else (
				match frame_array.(f) with
				| No_frame     -> extend_no_frame true (pred f)
				| Frame_I _    -> extend_no_frame false (pred f)
				| Frame_notI _ -> (
					if delete then frame_array.(f) <- No_frame;
					extend_no_frame delete (pred f)
				)
			) in
			extend_no_frame false (i.i_num_frames - 1)
		);

		(* Output the frames *)
		output_string out_stats_handle !options_line_ref; output_string out_stats_handle "\n";
		Array.iter (function
			| No_frame -> ()
			| Frame_I x | Frame_notI x -> (output_string out_stats_handle x; output_string out_stats_handle "\n")
		) frame_array;
		flush out_stats_handle;

		(* Save the frames to a bunch of ranges *)
		let rec increase_empty_range now prev_point = if now = i.i_num_frames then (
			let next_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = now; range_thresh = infinity} in
			add_range prev_point Range_empty next_point
		) else (
			match frame_array.(now) with
			| Frame_I _ -> (
				let next_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = now; range_thresh = infinity} in
				add_range prev_point Range_empty next_point;
				increase_full_range (succ now) next_point
			)
			| _ -> increase_empty_range (succ now) prev_point
		) and increase_full_range now prev_point = if now = i.i_num_frames then (
			let next_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = now; range_thresh = infinity} in
			add_range prev_point (Range_full (Array.sub frame_array prev_point.range_frame (now - prev_point.range_frame))) next_point
		) else (
			match frame_array.(now) with
			| No_frame -> (
				let next_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = (now - 1); range_thresh = infinity} in
				add_range prev_point (Range_full (Array.sub frame_array prev_point.range_frame (now - 1 - prev_point.range_frame))) next_point;
				increase_empty_range (succ now) next_point
			)
			| _ -> increase_full_range (succ now) prev_point
		) in

		let ranges = {range_prev = None; range_next = None; range_delfirst = false; range_frame = 0; range_thresh = infinity} in
		(match frame_array.(0) with
			| No_frame -> increase_empty_range 0 ranges
			| _ -> increase_full_range 0 ranges
		);

		(* Split the empty ranges based on the split points in the file *)
		let rec split_empty point_now next_splits = (
			match point_now with
			| {range_next = None} -> () (* DONE! *)
			| {range_frame = a; range_next = Some {range_type = Range_empty; range_right = ({range_frame = b} as point_next)}} -> (
				match next_splits with
				| [] -> () (* No more splits; DONE *)
				| (f,t) :: tl when f <= a + o.o_batch_length -> (
					(* first split point is before the beginning of the range, or so close to the beginning that it's unusable *)
					split_empty point_now tl
				)
				| (f,t) :: tl when f >= b -> (
					(* first split point is after the end of the range *)
					split_empty point_next next_splits
				)
				| (f,t) :: tl -> (
					(* a + o.o_batch_length < f < b *)
					let split_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = f; range_thresh = t} in
					add_range point_now Range_empty split_point;
					add_range split_point Range_empty point_next;
					split_empty split_point tl
				)
			)
			| {range_frame = a; range_next = Some {range_right = ({range_frame = b} as point_next)}} -> (
				match next_splits with
				| [] -> () (* No more splits; DONE *)
				| (f,t) :: tl when f >= b -> (
					(* first split point is after the end of the range *)
					split_empty point_next next_splits
				)
				| (f,t) :: tl -> (
					(* first split is somewhere in the range; but the range is not empty, so IGNORE *)
					split_empty point_now tl
				)
			)
		) in
		split_empty ranges split_list;

		(* Output all the frames to the output file *)
		let rec output_splits point = (
			if point.range_thresh < infinity then (
				(* Output the threshold as a #SPLIT line *)
				Printf.fprintf out_stats_handle "#SPLIT %d %f\n" point.range_frame point.range_thresh
			);
			match point with
			| {range_next = None} -> ()
			| {range_next = Some {range_right = next}} -> (
				output_splits next
			)
		) in
		output_splits ranges;
		Printf.fprintf out_stats_handle "#VERSION %d\n" out_version;
		flush out_stats_handle;

		(out_stats_handle, in_name, out_name, ranges)
	) in

	let frames_done_at_beginning = (
		let rec count_frames so_far = function
			| {range_next = None} -> so_far
			| {range_frame = a; range_next = Some {range_type = Range_full _; range_right = ({range_frame = b} as next)}} -> count_frames (so_far + b - a) next
			| {range_next = Some {range_right = next}} -> count_frames so_far next
		in
		print ~screen:false "Found %d frames already done\n" (count_frames 0 file_ranges);
		count_frames 0 file_ranges
	) in

	(* Splitter variables *)
	let range_list_mutex = Mutex.create () in
	let range_list_full_condition = Condition.create () in
	let range_list_empty_condition = Condition.create () in
	let range_list = file_ranges in

	(* New dynamic agent variables *)
	let agent_hash = Hashtbl.create 10 in
	let agent_hash_mutex = Mutex.create () in
	
	(* This is (or will be) used to determine when to take out more than the first GOP if a DELFIRST is encountered *)
	let max_gop_length_so_far_ref = ref (
		let rec find_largest_in_array a max_so_far last_i_frame_found now = (
			if now = Array.length a then (
				max (now - last_i_frame_found) max_so_far
			) else (
				match a.(now) with
				| Frame_I _ -> (
					find_largest_in_array a (max (now - last_i_frame_found) max_so_far) now (succ now)
				)
				| _ -> (
					find_largest_in_array a max_so_far last_i_frame_found (succ now)
				)
			)
		) in
		let rec iter_array biggest = function
			| {range_next = None} -> biggest
			| {range_next = Some {range_type = Range_full a; range_right = next}} -> iter_array (max biggest (find_largest_in_array a 0 0 0)) next
			| {range_next = Some {range_right = next}} -> iter_array biggest next
		in
		iter_array 0 range_list
	) in
	print ~screen:false "Largest gop length from file: %d\n" !max_gop_length_so_far_ref;
	(* And a function to update it *)
	let update_max_gop_length a = (
		let rec find_largest_in_array max_so_far last_i_frame_found now = (
			if now = Array.length a then (
				max (now - last_i_frame_found) max_so_far
			) else (
				match a.(now) with
				| Frame_I _ -> (
					find_largest_in_array (max (now - last_i_frame_found) max_so_far) now (succ now)
				)
				| _ -> (
					find_largest_in_array max_so_far last_i_frame_found (succ now)
				)
			)
		) in
		max_gop_length_so_far_ref := (find_largest_in_array !max_gop_length_so_far_ref 0 0)
	) in

	(* Make a function which will return the current batch length *)
	(* The batch length will get smaller as more frames are taken from empty *)
	(* The function is quadratic, with f(0)=o.o_batch_length, f'(0)=0, f(i.i_num_frames)=o.o_batch_length*final_batch_mult *)
	let batch_now_unsafe = (
		let first_batch_float = float_of_int first_batch in
		let num_frames_float = float_of_int i.i_num_frames in
		let rec find_empty so_far = function
			| {range_next = None} -> so_far
			| {range_frame = a; range_next = Some {range_type = Range_empty; range_right = ({range_frame = b} as next)}} -> find_empty (so_far + b - a) next
			| {range_next = Some {range_right = next}} -> find_empty so_far next
		in
		fun () -> (
			let not_empty = i.i_num_frames - find_empty 0 range_list in
			let not_empty_float = float_of_int not_empty in
			max (!max_gop_length_so_far_ref + 1) (first_batch + int_of_float (((o.o_final_batch_mult -. 1.0) *. first_batch_float *. not_empty_float *. not_empty_float) /. (num_frames_float *. num_frames_float)))
		)
	) in
	let batch_now = (
		Mutex.lock range_list_mutex;
		let a = batch_now_unsafe () in
		Mutex.unlock range_list_mutex;
		a
	) in

	(* A handy function for getting the range with the largest start thresh which matches the specified criteria *)
	let find_largest_valid_thresh ?(above=0.0) func ranges = (
		let rec helper point_now largest_point_option = (
			if point_now.range_thresh >= above && func point_now then (
				(* Matches *)
				let now_largest_point = match largest_point_option with
					| Some x when x.range_thresh > point_now.range_thresh -> x
					| _ -> point_now
				in
				match point_now with
				| {range_next = Some {range_right = next}} -> helper next (Some now_largest_point)
				| _ -> Some now_largest_point
			) else (
				(* Does not match *)
				match point_now with
				| {range_next = Some {range_right = next}} -> helper next largest_point_option
				| _ -> largest_point_option
			)
		) in
		helper ranges None
	) in

	(* RANGE PRINTING *)
	let print_range_width = match i.i_num_frames with
		| x when x < 10       -> 1
		| x when x < 100      -> 2
		| x when x < 1000     -> 3
		| x when x < 10000    -> 4
		| x when x < 100000   -> 5
		| x when x < 1000000  -> 6
		| x when x < 10000000 -> 7
		| x                   -> 8
	in

	(* RANGE PRINTING *)
	let rec print_ranges_unsafe point = (
		print ~lock:false ~screen:false " %*d @ %s%s\n" print_range_width point.range_frame (match classify_float point.range_thresh with | FP_infinite -> "INFINITY" | _ -> Printf.sprintf "%.2f" point.range_thresh) (if point.range_delfirst then " INCORRECT" else "");
		match point with
		| {range_next = Some r} -> (
			print ~lock:false ~screen:false " %*s %s\n" print_range_width "" (match r.range_type with | Range_empty -> "Empty" | Range_full _ -> "Full" | Range_queued -> "Queued" | Range_working s -> "Working " ^ s);
			print_ranges_unsafe r.range_right
		)
		| _ -> () (* Done! *)
	) in

	let print_ranges message = (
		Mutex.lock pm;
		if message <> "" then print ~lock:false ~screen:false "%s" message;
		print_ranges_unsafe range_list;
		Mutex.unlock pm;
	) in
(*
	print_ranges "From file:\n";
	exit 673;
*)

(*
	let print_ranges message = (
		Mutex.lock pm;
		print ~lock:false "%s" message;
		print_ranges_unsafe !range_list_ref;
		Mutex.unlock pm;
	) in
	print_ranges "From file:\n";
*)
(*	exit 4;*)

	(****************)
	(* NEW PRINTING *)
	(****************)
	let p_term_width = if use_console_functions then (
		let info = Console.get_console_screen_buffer_info Console.std_output_handle in
		info.Console.dwSizeX
	) else 80 in

	(* Variables handling error printing *)
	let p_error_array_mutex = Mutex.create () in
	let p_error_length = 4 in
	let p_error_oldest_ref = ref 0 in (* The index of the oldest error in the array *)
	let p_error_array = Array.make p_error_length ("~","") in (* The error array is actually cyclical *)
	let p_error_array_temp = Array.make p_error_length ("","") in (* This is only used for copying from error_array, to avoid needing two locks at the same time *)

	(* This handles the space available for the printing *)
	let p_lines_overhead = p_error_length + 5 in (* p_error_lines + number of agents = total lines *)
	let p_current_lines_ref = ref 0 in (* This is the current number of lines allocated to the printer *)

	let time_of_seconds eta = match classify_float eta with
		| FP_normal | FP_subnormal | FP_zero -> (
			let eta_int = int_of_float eta in
			let eta_sec = eta_int mod 60 in
			let eta_min = (eta_int / 60) mod 60 in
			let eta_hr  = (eta_int / 3600) mod 24 in
			let eta_dy  = (eta_int / 86400) mod 7 in
			let eta_wk  = eta_int / 604800 in
			(match (eta_wk, eta_dy, eta_hr) with
				| (0,0,0) -> Printf.sprintf "%d:%02d" eta_min eta_sec;
				| (0,0,_) -> Printf.sprintf "%d:%02d:%02d" eta_hr eta_min eta_sec;
				| (0,_,_) -> Printf.sprintf "%dd %d:%02d:%02d" eta_dy eta_hr eta_min eta_sec;
				| (_,_,_) -> Printf.sprintf "%dw %dd %d:%02d:%02d" eta_wk eta_dy eta_hr eta_min eta_sec;
			);
		)
		| _ -> (
			"?";
		)
	in

	let print_everything () = (

		(* Range string *)
		let str = String.make (p_term_width - 2) '#' in
		let strlen = String.length str in
		let rec fill_range_unsafe = function
			| {range_next = None} -> ()
			| {range_thresh = thresh; range_frame = a; range_delfirst = delfirst; range_next = Some ({range_right = ({range_frame = b} as next)} as r)} -> (
				let c_from = a * strlen / i.i_num_frames in
				let c_to = (b - 1) * strlen / i.i_num_frames in
				(match r.range_type with
					| Range_full _ -> () (* Ignore; the string defaults to full anyway *)
					| Range_empty -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' -> str.[c] <- '.'
							| _ -> ()
						done
					)
					| Range_queued when thresh < o.o_rethresh -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' -> str.[c] <- '_'
							| _ -> ()
						done
					)
					| Range_queued -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' -> str.[c] <- '-'
							| _ -> ()
						done
					)
					| Range_working _ when delfirst -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' | '_' -> str.[c] <- '!'
							| _ -> ()
						done
					)
					| Range_working _ -> (
						for c = c_from to c_to do
							str.[c] <- '|'
						done
					)
				);
				fill_range_unsafe next
			)
		in
		Mutex.lock range_list_mutex;
		fill_range_unsafe range_list;
		Mutex.lock pm;
		print_ranges_unsafe range_list;
		Mutex.unlock pm;
		Mutex.unlock range_list_mutex;

		(* FPS line *)
		let fps_line = (
			let rec count_frames (e,f,q,w) = function
				| {range_next = None} -> (e,f,q,w)
				| {range_frame = a; range_next = Some ({range_right = ({range_frame = b} as next)} as r)} -> (
					match r.range_type with
					| Range_empty     -> count_frames (e + b - a, f, q, w) next
					| Range_full _    -> count_frames (e, f + b - a, q, w) next
					| Range_queued    -> count_frames (e, f, q + b - a, w) next
					| Range_working _ -> count_frames (e, f, q, w + b - a) next
				)
			in
			let (e,f,q,w) = count_frames (0,0,0,0) range_list in
			let fps = (float_of_int (f - frames_done_at_beginning) /. (Sys.time () -. first_pass_start_time)) in
			Printf.sprintf "%d%% done (%d / %d) at %.2f FPS" (int_of_float (100.0 *. (float_of_int f /. float_of_int i.i_num_frames))) f i.i_num_frames fps;
		) in

		(* Time taken *)
		let time_so_far_line = Printf.sprintf "Last updated: %s" (time_of_seconds (Sys.time ())) in

		(* Disher status *)
		(* New disher status based off of agent_status_t *)
		(* string_list has type (string * string * string) list, for (name, fps, status) per computer *)
		Mutex.lock agent_hash_mutex;
		let (name_list, fps_list, status_list_list) = Hashtbl.fold (fun (ip,port) (name,agent_array) (n,f,s) ->
			let (fps, status_list) = Array.fold_left (fun (fps_so_far,status_so_far) gnu ->
				((if gnu.a1_total_time = 0.0 then fps_so_far else fps_so_far +. float_of_int gnu.a1_total_frames /. gnu.a1_total_time), gnu.a1_status :: status_so_far)
			) (0.0,[]) agent_array in
			let fps_string = Printf.sprintf "%.2f" fps in
			(name :: n, fps_string :: f, status_list :: s)
		) agent_hash ([],[],[]) in
		Mutex.unlock agent_hash_mutex;
		(* Now parse the status list list *)
		let status_string_list = List.map (fun status_list ->
			let (disi,conl,waii,chel,senl,doil,deai,donl,fail,exii) = status_splitter status_list in
			let sens = List.fold_left (fun s (a,b) -> (if s = "" then "" else s ^ ",") ^ Printf.sprintf "[%d-%d]" a b) "" senl in
			let dois = List.fold_left (fun s (a,b) -> (if s = "" then "" else s ^ ",") ^ Printf.sprintf "[%d-%d]" a b) "" doil in
			let ches = List.fold_left (fun s (a,b) -> (if s = "" then "" else s ^ ",") ^ Printf.sprintf "[%d-%d]" a b) "" chel in
			let wais = if waii = 0 then "" else string_of_int waii in
			let dons = List.fold_left (fun s (a,b) -> (if s = "" then "" else s ^ ",") ^ Printf.sprintf "[%d-%d]" a b) "" donl in
			let fais = List.fold_left (fun s (a,b) -> (if s = "" then "" else s ^ ",") ^ Printf.sprintf "[%d-%d]" a b) "" fail in
			let exis = if exii = 0 then "" else string_of_int exii in
			let deas = if deai = 0 then "" else string_of_int deai in
			let cons = match conl with | [] -> "" | (a,b) :: _ -> Printf.sprintf "%s:%d" a b in
			let diss = if disi = 0 then "" else string_of_int disi in
			let a = if sens = "" then "" else "Sending " ^ sens in
			let a = if dois = "" then a else if a = "" then "Doing " ^ dois else a ^ "; doing " ^ dois in
			let a = if ches = "" then a else if a = "" then "Checked out " ^ ches  else a in
			let a = if wais = "" then a else if a = "" then "Waiting for job"      else a in
			let a = if dons = "" then a else if a = "" then "Done with " ^ dons    else a in
			let a = if fais = "" then a else if a = "" then fais ^ " failed"       else a in
			let a = if exis = "" then a else if a = "" then "Exited"               else a in
			let a = if deas = "" then a else if a = "" then "Dead?"                else a in
			let a = if cons = "" then a else if a = "" then "Connected to " ^ cons else a in
			let a = if diss = "" then a else if a = "" then "Disconnected"         else a in
			a
(*
			if List.exists (function | S_Sending _ -> true | _ -> false) status_list then (
				(* At least one agent is sending data (controller-based) *)
				"Something's sending"
			) else if List.exists (function | S_Doing _ -> true | _ -> false) status_list then (
				(* Agent-based *)
				"Something's doing"
			) else if List.exists (function | S_Checked_out _ -> true | _ -> false) status_list then (
				"Something's checked out"
			) else if List.exists (function | S_Waiting -> true | _ -> false) status_list then (
				"Something's waiting"
			) else if List.exists (function | S_Done _ -> true | _ -> false) status_list then (
				"Something's done"
			) else if List.exists (function | S_Failed _ -> true | _ -> false) status_list then (
				"Something's failed"
			) else if List.exists (function | S_Exited -> true | _ -> false) status_list then (
				"Something's exited"
			) else if List.exists (function | S_Dead -> true | _ -> false) status_list then (
				"Something's dead"
			) else if List.exists (function | S_Connected _ -> true | _ -> false) status_list then (
				"Something's connected"
			) else if List.exists (function | S_Disconnected -> true | _ -> false) status_list then (
				"Something's disconnected"
			) else (
				"HUH?"
			)
*)
		) status_list_list in
		(* Merge *)
		let rec merge n f s = match (n,f,s) with
			| (na::nb,fa::fb,sa::sb) -> (na,fa,sa) :: merge nb fb sb
			| ([],[],[]) -> []
			| _ -> failwith "Printing error: lists not the same length"
		in
		let string_list = merge name_list fps_list status_string_list in
(*
		Mutex.lock agent_hash_mutex;
		let string_list = Hashtbl.fold (fun (ip,port) (name,agent_array) list_so_far ->
			let (fps, status) = Array.fold_left (fun (fps_so_far, status_so_far) n ->
				let new_status = match status_so_far with
					| "" -> n.a1_status
					| _ -> status_so_far ^ " ~ " ^ n.a1_status
				in
				(fps_so_far +. float_of_int n.a1_total_frames /. n.a1_total_time, new_status)
			) (0.0, "") agent_array in
			(*let fps = if time <= 0.0 then "?" else Printf.sprintf "%.2f" (float_of_int frames /. time) in*)
			let fps_string = match classify_float fps with
				| FP_nan | FP_infinite -> "?"
				| _ -> Printf.sprintf "%.2f" fps
			in
			(name, fps_string, status) :: list_so_far
		) agent_hash [] in
		Mutex.unlock agent_hash_mutex;
*)
		let (num_agents, max_name, max_fps) = List.fold_left (fun (a_num,n_num,f_num) (n,f,_) ->
			(succ a_num, max n_num (String.length n), max f_num (String.length f))
		) (0,5,3) string_list in
		let max_status = p_term_width - 2 - 3 - 3 - max_name - max_fps in
		

		(* Error array *)
		Mutex.lock p_error_array_mutex;
		for i = 0 to (pred p_error_length) do
			p_error_array_temp.(i) <- p_error_array.((i + !p_error_oldest_ref) mod p_error_length)
		done;
		Mutex.unlock p_error_array_mutex;
		let max_error_time_length = Array.fold_left (fun so_far (gnu,_) -> max so_far (String.length gnu)) 0 p_error_array_temp in

		(* Print everything *)
		Mutex.lock pm;
		if use_console_functions then (
			(* Figure how much space is necessary *)
			let add_lines = p_lines_overhead + num_agents - !p_current_lines_ref in
			if add_lines > 0 then (
				for i = 1 to add_lines do
					print ~dump:false "\n";
				done;
				p_current_lines_ref := !p_current_lines_ref + add_lines
			);
			let info = Console.get_console_screen_buffer_info Console.std_output_handle in
			Console.set_console_cursor_position Console.std_output_handle (0, info.Console.dwCursorY - !p_current_lines_ref);
		);
		print ~lock:false ~fill:true " %s" fps_line;
		print ~lock:false ~fill:true " %s" time_so_far_line;
		print ~lock:false ~fill:true " %s" str;
		print ~lock:false ~fill:true " %*s | %-*s | Description" max_name "Agent" max_fps "FPS";
		List.iter (fun (name, fps, status) ->
			print ~lock:false ~fill:true " %*s | %-*s | %s" max_name name max_fps fps (if String.length status > max_status then String.sub status 0 (max_status - 3) ^ "..." else status)
		) string_list;
		print ~lock:false ~fill:true "Recent errors:";
		Array.iter (fun (t,e) -> let x = Printf.sprintf "%*s %s" max_error_time_length t e in print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 5) ^ "..." else x)) p_error_array_temp;
		Mutex.unlock pm;
	) in

	(* Don't need print_status anymore... *)

	let update_status disher_key disher_index status = (
		Mutex.lock agent_hash_mutex;
		let old_status = (snd (Hashtbl.find agent_hash disher_key)).(disher_index).a1_status in
		Mutex.unlock agent_hash_mutex;
		if status <> old_status then (
			(* Lock it again! (there's probably a better way to do this...) *)
			Mutex.lock agent_hash_mutex;
			(snd (Hashtbl.find agent_hash disher_key)).(disher_index).a1_status <- status;
			Mutex.unlock agent_hash_mutex;
			print_everything ()
		)
	) in
		
	let add_error error = (
		Mutex.lock p_error_array_mutex;
		p_error_array.(!p_error_oldest_ref) <- (time_of_seconds (Sys.time ()), error);
		p_error_oldest_ref := (succ !p_error_oldest_ref) mod p_error_length;
		Mutex.unlock p_error_array_mutex;
	) in

	let fps_disher_start disher_key disher_index = (
		Mutex.lock agent_hash_mutex;
		(snd (Hashtbl.find agent_hash disher_key)).(disher_index).a1_last_start <- Sys.time ();
		Mutex.unlock agent_hash_mutex;
	) in
	let fps_disher_stop disher_key disher_index frames = (
		Mutex.lock agent_hash_mutex;
		let a = (snd (Hashtbl.find agent_hash disher_key)).(disher_index) in
		a.a1_total_frames <- a.a1_total_frames + frames;
		a.a1_total_time <- a.a1_total_time +. Sys.time () -. a.a1_last_start;
		Mutex.unlock agent_hash_mutex;
	) in

	(*******************)
	(* STATIC PRINTING *)
	(*******************)
(*
	let p_disher_names = Array.map (fun (name,(ip,pf,pt),n) -> name ^ " " ^ string_of_int n) o.o_agents_1 in
	let p_disher_status_mutex = Mutex.create () in
	let p_disher_status = Array.map (fun _ -> "Disconnected") o.o_agents_1 in

	let p_term_width = if use_console_functions then (
		let info = Console.get_console_screen_buffer_info Console.std_output_handle in
		info.Console.dwSizeX
	) else 80 in

	(* Variables handling error printing *)
	let p_error_array_mutex = Mutex.create () in
	let p_error_length = 4 in
	let p_error_oldest_ref = ref 0 in (* The index of the oldest error in the array *)
	let p_error_array = Array.make p_error_length ("~","") in (* The error array is actually cyclical *)
	let p_error_array_temp = Array.make p_error_length ("","") in (* This is only used for copying from error_array, to avoid needing two locks at the same time *)

	(* Variables handling per-agent FPS counts *)
	let p_fps_array_mutex = Mutex.create () in
	let p_fps_frames = Array.map (fun _ -> 0) o.o_agents_1 in
	let p_fps_time = Array.map (fun _ -> 0.0) o.o_agents_1 in
	let p_fps_last_disher_start = Array.map (fun _ -> 0.0) o.o_agents_1 in

	(* Add the proper amount of padding in the output *)
	let p_lines = Array.length o.o_agents_1 + p_error_length + 5 in
(*	print "THIS SHOULD NOT BE OVERWRITTEN!\n";*)
	if use_console_functions then (
		for i = 1 to p_lines do
			print ~dump:false "\n";
		done
	);

	let time_of_seconds eta = match classify_float eta with
		| FP_normal | FP_subnormal | FP_zero -> (
			let eta_int = int_of_float eta in
			let eta_sec = eta_int mod 60 in
			let eta_min = (eta_int / 60) mod 60 in
			let eta_hr  = (eta_int / 3600) mod 24 in
			let eta_dy  = (eta_int / 86400) mod 7 in
			let eta_wk  = eta_int / 604800 in
			(match (eta_wk, eta_dy, eta_hr) with
				| (0,0,0) -> Printf.sprintf "%d:%02d" eta_min eta_sec;
				| (0,0,_) -> Printf.sprintf "%d:%02d:%02d" eta_hr eta_min eta_sec;
				| (0,_,_) -> Printf.sprintf "%dd %d:%02d:%02d" eta_dy eta_hr eta_min eta_sec;
				| (_,_,_) -> Printf.sprintf "%dw %dd %d:%02d:%02d" eta_wk eta_dy eta_hr eta_min eta_sec;
			);
		)
		| _ -> (
			"?";
		)
	in

	let print_everything () = (
		let str = String.make (p_term_width - 2) '#' in
		let strlen = String.length str in

		let rec fill_range_unsafe = function
			| {range_next = None} -> ()
			| {range_thresh = thresh; range_frame = a; range_delfirst = delfirst; range_next = Some ({range_right = ({range_frame = b} as next)} as r)} -> (
				let c_from = a * strlen / i.i_num_frames in
				let c_to = (b - 1) * strlen / i.i_num_frames in
				(match r.range_type with
					| Range_full _ -> () (* Ignore; the string defaults to full anyway *)
					| Range_empty -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' -> str.[c] <- '.'
							| _ -> ()
						done
					)
					| Range_queued when thresh < o.o_rethresh -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' -> str.[c] <- '_'
							| _ -> ()
						done
					)
					| Range_queued -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' -> str.[c] <- '-'
							| _ -> ()
						done
					)
					| Range_working _ when delfirst -> (
						for c = c_from to c_to do
							match str.[c] with
							| '#' | '.' | '_' -> str.[c] <- '!'
							| _ -> ()
						done
					)
					| Range_working _ -> (
						for c = c_from to c_to do
							str.[c] <- '|'
						done
					)
				);
				fill_range_unsafe next
			)
		in


		Mutex.lock range_list_mutex;
		fill_range_unsafe range_list;

		Mutex.lock pm;
		print_ranges_unsafe range_list;
		Mutex.unlock pm;

		Mutex.unlock range_list_mutex;

		let fps_line = (
			let rec count_frames (e,f,q,w) = function
				| {range_next = None} -> (e,f,q,w)
				| {range_frame = a; range_next = Some ({range_right = ({range_frame = b} as next)} as r)} -> (
					match r.range_type with
					| Range_empty     -> count_frames (e + b - a, f, q, w) next
					| Range_full _    -> count_frames (e, f + b - a, q, w) next
					| Range_queued    -> count_frames (e, f, q + b - a, w) next
					| Range_working _ -> count_frames (e, f, q, w + b - a) next
				)
			in
			let (e,f,q,w) = count_frames (0,0,0,0) range_list in
			let fps = (float_of_int (f - frames_done_at_beginning) /. (Sys.time () -. first_pass_start_time)) in
			Printf.sprintf "%d%% done (%d / %d) at %.2f FPS" (int_of_float (100.0 *. (float_of_int f /. float_of_int i.i_num_frames))) f i.i_num_frames fps;
		) in

		(* Time taken *)
		let time_so_far_line = Printf.sprintf "Last updated: %s" (time_of_seconds (Sys.time ())) in

		(* Disher status *)
		(* Start with length 5 for the string "Agent" *)
		let max_disher_length = Array.fold_left (fun so_far gnu -> max (String.length gnu) so_far) 5 p_disher_names in

		Mutex.lock p_fps_array_mutex;
		let fps_string_array = Array.mapi (fun i time -> if time <= 0.0 then "?" else Printf.sprintf "%.2f" (float_of_int p_fps_frames.(i) /. time)) p_fps_time in
		Mutex.unlock p_fps_array_mutex;
		let max_fps_length = Array.fold_left (fun so_far gnu -> max so_far (String.length gnu)) 3 fps_string_array in

		Mutex.lock p_disher_status_mutex;
		let disher_strings = Array.init (Array.length o.o_agents_1) (fun x ->
			Printf.sprintf "%*s | %-*s | %s" max_disher_length p_disher_names.(x) max_fps_length fps_string_array.(x) p_disher_status.(x)
		) in
		Mutex.unlock p_disher_status_mutex;

		Mutex.lock p_error_array_mutex;
		for i = 0 to (pred p_error_length) do
			p_error_array_temp.(i) <- p_error_array.((i + !p_error_oldest_ref) mod p_error_length)
		done;
		Mutex.unlock p_error_array_mutex;
		let max_time_length = Array.fold_left (fun so_far (gnu,_) -> max so_far (String.length gnu)) 0 p_error_array_temp in

		(* Print everything! *)
		Mutex.lock pm;
		if use_console_functions then (
			let info = Console.get_console_screen_buffer_info Console.std_output_handle in
			Console.set_console_cursor_position Console.std_output_handle (0, info.Console.dwCursorY - p_lines);
		);
		print ~lock:false ~fill:true " %s" fps_line;
		print ~lock:false ~fill:true " %s" time_so_far_line;
		print ~lock:false ~fill:true " %s" str;
		print ~lock:false ~fill:true " %*s | %-*s | Description" max_disher_length "Agent" max_fps_length "FPS";
		Array.iter (fun x -> print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) disher_strings;
		print ~lock:false ~fill:true "Recent errors:";
(*		Array.iter (fun x -> print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) p_error_array_temp;*)

		Array.iter (fun (t,e) -> let x = Printf.sprintf "%*s %s" max_time_length t e in print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) p_error_array_temp;

		Mutex.unlock pm;

	) in

	let print_status () = (
(*
		Mutex.lock range_list_mutex;
		let current_range_list = !range_list_ref in
		Mutex.unlock range_list_mutex;
*)
(*
		Mutex.lock pm;
		print ~lock:false "%s" message;
		print_ranges_unsafe !range_list_ref;
		Mutex.unlock pm;
*)

		print_everything ();

	) in

	(* Update the data and print if needed *)
	let update_status disher status = (
		if status <> p_disher_status.(disher) then (
			Mutex.lock p_disher_status_mutex;
			p_disher_status.(disher) <- status;
			Mutex.unlock p_disher_status_mutex;
			print_status ()
		)
	) in

	let update_range_list () = (
		print_everything ();
	) in

	(* Add an error to the error queue *)
	let add_error error = (
		Mutex.lock p_error_array_mutex;
		p_error_array.(!p_error_oldest_ref) <- (time_of_seconds (Sys.time ()), error);
		p_error_oldest_ref := (succ !p_error_oldest_ref) mod p_error_length;
		Mutex.unlock p_error_array_mutex;
		print_status ()
	) in

	(* Update the FPS *)
	let fps_disher_start n = (
		Mutex.lock p_fps_array_mutex;
		p_fps_last_disher_start.(n) <- Sys.time ();
		Mutex.unlock p_fps_array_mutex;
	) in
	let fps_disher_stop n frames = (
		Mutex.lock p_fps_array_mutex;
		p_fps_time.(n) <- p_fps_time.(n) +. Sys.time () -. p_fps_last_disher_start.(n);
		p_fps_frames.(n) <- p_fps_frames.(n) + frames;
		Mutex.unlock p_fps_array_mutex;
	) in

	let print_ranges _ = () in
	print_ranges "From file:\n";
*)

	(************)
	(* SPLITTER *)
	(************)
	(* This function now returns the threshold of the frames found *)
	let rec check_nth_frame start_frame end_frame prev_frame_guts total_diff_so_far max_frame_diff max_frame_num current_frame_offset = (
		if current_frame_offset > o.o_split_frame_length || start_frame + current_frame_offset > end_frame then (
			{range_prev = None; range_next = None; range_delfirst = false; range_frame = start_frame + current_frame_offset; range_thresh = 1.0}
		) else (
			let current_threshold = (o.o_split_thresh -. 1.0) *. float_of_int (current_frame_offset - 1) /. float_of_int (1 - o.o_split_frame_length) +. o.o_split_thresh in
			
			let now_frame_guts = i.i_get_fast_frame_y (start_frame + current_frame_offset) in
			let diff_now = i.i_fast_frame_diff prev_frame_guts now_frame_guts in
			
			let new_total_diff = total_diff_so_far +. (float_of_int diff_now) in (* floats are ~2x faster than int64s on Win32. (int64s are ~50% faster than floats on 64-bit Linux, though...) *)
			let new_average_diff = new_total_diff /. float_of_int current_frame_offset in


			let (now_max_diff, now_max_num) = if diff_now > max_frame_diff
				then (diff_now, current_frame_offset)
				else (max_frame_diff, max_frame_num)
			in

(*			let current_multiple = float_of_int diff_now /. new_average_diff in*)
(*print " %-3d frame %3d\n  Current thresh %5.2f\n  Current diff %d\n  Avg diff %f\n  Max diff %d@%d) (or %d@%d)\n" (start_frame + current_frame_offset) current_frame_offset current_threshold diff_now new_average_diff max_frame_diff max_frame_num now_max_diff now_max_num;*)
			
			let max_multiple = float_of_int now_max_diff /. new_average_diff in
			
			if max_multiple >= current_threshold || current_threshold < 1.0 then (
				{range_prev = None; range_next = None; range_delfirst = false; range_frame = start_frame + now_max_num; range_thresh = max_multiple}
			) else (
				check_nth_frame start_frame end_frame now_frame_guts new_total_diff now_max_diff now_max_num (succ current_frame_offset)
			)
		)
	) in

	let splitter_guts () = (
		let print_mutex = (fun a -> print ~screen:false ~name:"SPLITTER MUTEX " a) in
		let print = (fun a -> print ~screen:false a) in
	
		print "Rethresh = %f\n" o.o_rethresh;
		
		let rec qcount_all_ranges (e,f,q,w) = function
			| {range_next = Some {range_type = Range_empty    ; range_right = n}} -> qcount_all_ranges (succ e, f, q, w) n
			| {range_next = Some {range_type = Range_full _   ; range_right = n}} -> qcount_all_ranges (e, succ f, q, w) n
			| {range_next = Some {range_type = Range_queued   ; range_right = n}} -> qcount_all_ranges (e, f, succ q, w) n
			| {range_next = Some {range_type = Range_working _; range_right = n}} -> qcount_all_ranges (e, f, q, succ w) n
			| {range_next = None} -> (e,f,q,w)
		in
		
		(* This counts all ranges greater or equal to x *)
		(* It puts the ranges smaller than x in the second part of the tuple *)
		let rec count_all_ranges_larger x ((e,f,q,w),(es,fs,qs,ws)) = function
			| {range_next = Some {range_type = Range_empty    ; range_right = n}; range_thresh = x2} when x2 < x -> count_all_ranges_larger x ((e,f,q,w), (succ es, fs, qs, ws)) n
			| {range_next = Some {range_type = Range_full _   ; range_right = n}; range_thresh = x2} when x2 < x -> count_all_ranges_larger x ((e,f,q,w), (es, succ fs, qs, ws)) n
			| {range_next = Some {range_type = Range_queued   ; range_right = n}; range_thresh = x2} when x2 < x -> count_all_ranges_larger x ((e,f,q,w), (es, fs, succ qs, ws)) n
			| {range_next = Some {range_type = Range_working _; range_right = n}; range_thresh = x2} when x2 < x -> count_all_ranges_larger x ((e,f,q,w), (es, fs, qs, succ ws)) n
			| {range_next = Some {range_type = Range_empty    ; range_right = n}} -> count_all_ranges_larger x ((succ e, f, q, w), (es,fs,qs,ws)) n
			| {range_next = Some {range_type = Range_full _   ; range_right = n}} -> count_all_ranges_larger x ((e, succ f, q, w), (es,fs,qs,ws)) n
			| {range_next = Some {range_type = Range_queued   ; range_right = n}} -> count_all_ranges_larger x ((e, f, succ q, w), (es,fs,qs,ws)) n
			| {range_next = Some {range_type = Range_working _; range_right = n}} -> count_all_ranges_larger x ((e, f, q, succ w), (es,fs,qs,ws)) n
			| {range_next = None} -> ((e,f,q,w),(es,fs,qs,ws))
		in

		(* All these functions return whether they did anything or not (since the assignment is set with side-effects now) *)
		(* Queue the first frames, since this isn't caught anywhere else and it's a great place to start *)
		let queue_beginning = function
			| {range_frame = a; range_next = Some ({range_type = Range_empty; range_right = {range_frame = b}} as r)} -> (
				if b - a < o.o_batch_length + o.o_split_frame_length then (
					(* Queue the whole thing *)
					add_range r.range_left Range_queued r.range_right;
					true
				) else (
					let start_frame = o.o_batch_length in
					let new_point = check_nth_frame start_frame (pred r.range_right.range_frame) (i.i_get_fast_frame_y start_frame) 0.0 0 1 1 in
					Printf.fprintf out_stats_handle "#SPLIT %d %f\n" new_point.range_frame new_point.range_thresh;
					add_range r.range_left Range_queued new_point;
					add_range new_point Range_empty r.range_right;
					true
				)
			)
			| _ -> (false) (* No ranges? First range not empty? Or something? Who cares? *)
		in

		(* Make another range right after a finished range and queue it *)
		(* Also queue after working ranges, which makes it easier to keep going on a particular range *)
		let rec queue_after_full_or_working batch_length p1 = 
			print "batch length: %d\n" batch_length;
			match p1 with
			| {range_frame = a; range_prev = ((Some {range_type = (Range_full _ | Range_working _)}) | None); range_next = Some {range_type = Range_empty; range_right = ({range_frame = b} as p2)}} -> (
				(* Match all the points which have a full range before it and an empty range after it *)
				(* Or nothing before and empty range after *)
				match p2 with
				| {range_delfirst = true} when b - a <= batch_length + o.o_split_frame_length -> (
					(* Small range, but the next point is incorrect; ignore *)
					print "queue_after_full_or_working found small range (%d,%d) but the next point is incorrect\n" a b;
					queue_after_full_or_working batch_length p2
				)
				| _ when b - a <= batch_length + o.o_split_frame_length -> (
					(* Small range after a done range. Queue it! *)
					print "queue_after_full_or_working found and queued small range (%d,%d)\n" a b;
					add_range p1 Range_queued p2;
(*					queue_after_full_or_working true p2*)
					true
				)
				| _ when b - a <= batch_length + o.o_split_frame_length -> (
					(* Small range after a done range. Queue it! (Separate since the next range does not exist) *)
					print "queue_after_full_or_working found and queued small last range (%d,%d)\n" a b;
					add_range p1 Range_queued p2;
(*					queue_after_full_or_working true p2*)
					true
				)
				| _ when b - a <= 2 * batch_length + o.o_split_frame_length -> (
					(* Sorta small range; split down the middle and queue the first half *)
					let start_frame = max (a + !max_gop_length_so_far_ref + 1) ((a + b - batch_length) / 2) in (* Try to split around the middle of the range, except if the max GOP length gets in the way *)
					let new_point = check_nth_frame start_frame (pred b) (i.i_get_fast_frame_y start_frame) 0.0 0 1 1 in
					Printf.fprintf out_stats_handle "#SPLIT %d %f\n" new_point.range_frame new_point.range_thresh;
					if new_point.range_frame >= (pred b) then (
						match p2 with
						| {range_delfirst = true} -> (
							print "queue_after_full_or_working found a fairly small range (%d,%d) but failed to split it, and the next point is incorrect\n" a b;
							queue_after_full_or_working batch_length p2
						)
						| _ -> (
							print "queue_after_full_or_working found a fairly small range (%d,%d) but failed to split it; queueing the whole thing\n" a b;
							add_range p1 Range_queued p2;
(*							queue_after_full_or_working true p2*)
							true
						)
					) else (
						print "queue_after_full_or_working found a fairly small range (%d,%d) and split at %d\n" a b new_point.range_frame;
						add_range p1 Range_queued new_point;
						add_range new_point Range_empty p2;
(*						queue_after_full_or_working true p2*)
						true
					)
				)
				| _ -> (
					(* Split off the beginning *)
					let start_frame = a + batch_length in
					let new_point = check_nth_frame start_frame (pred b) (i.i_get_fast_frame_y start_frame) 0.0 0 1 1 in
					Printf.fprintf out_stats_handle "#SPLIT %d %f\n" new_point.range_frame new_point.range_thresh;
					if new_point.range_frame >= pred b then (
						match p2 with
						| {range_delfirst = true} -> (
							print "queue_after_full_or_working found a big range (%d,%d), but couldn't split it, and the next point is incorrect\n" a b;
							queue_after_full_or_working batch_length p2
						)
						| _ -> (
							print "queue_after_full_or_working found a big range (%d,%d), but couldn't split it, queue the whole thing\n" a b;
							add_range p1 Range_queued p2;
(*							queue_after_full_or_working true p2*)
							true
						)
					) else (
						print "queue_after_full_or_working found a big range (%d,%d) and split at %d\n" a b new_point.range_frame;
						add_range p1 Range_queued new_point;
						add_range new_point Range_empty p2;
(*						queue_after_full_or_working true p2*)
						true
					)
				)
			)
			| {range_next = Some {range_right = next}} -> queue_after_full_or_working batch_length next
			| _ -> false
		in

		(* This takes an empty range followed by a queued range and queues the last part of the empty range *)
		let extend_small_thresh_queue batch_length ranges = (
			let rec most_pathetic_queue so_far gnu = (match gnu with
				| {range_next = None} -> so_far
				| {range_delfirst = false; range_thresh = t; range_prev = Some {range_type = Range_empty}; range_next = Some {range_type = Range_queued; range_right = next}} -> (
					match so_far with
					| Some {range_thresh = t_so_far} when t_so_far < t -> most_pathetic_queue so_far next
					| _ -> most_pathetic_queue (Some gnu) next
				)
				| {range_next = Some {range_right = next}} -> most_pathetic_queue so_far next
			) in
			match most_pathetic_queue None ranges with
			| Some ({range_thresh = thresh; range_prev = Some {range_left = p1}} as p2) when thresh < o.o_rethresh -> (
				print "extend_small_thresh_queue found pathetic range\n";
				if p2.range_frame - p1.range_frame <= batch_length + o.o_split_frame_length then (
					(* Small enough to queue the whole thing *)
					add_range p1 Range_queued p2;
					true
				) else (
					(* Split somewhere *)
					let start_frame = (
						if p2.range_frame - p1.range_frame <= 2 * batch_length + o.o_split_frame_length then (
							(* Smallish range; split around the middle *)
							min (p2.range_frame - (!max_gop_length_so_far_ref + 1 + o.o_split_frame_length)) ((p2.range_frame + p1.range_frame - o.o_split_frame_length) / 2)
						) else (
							(* Large range; split toward the end *)
							p2.range_frame - batch_length - o.o_split_frame_length
						)
					) in
					let new_point = check_nth_frame start_frame (pred p2.range_frame) (i.i_get_fast_frame_y start_frame) 0.0 0 1 1 in
					Printf.fprintf out_stats_handle "#SPLIT %d %f\n" new_point.range_frame new_point.range_thresh;
					if new_point.range_frame >= (pred p2.range_frame) then (
						(* I don't think this can happen, but that's OK *)
						add_range p1 Range_queued p2;
						true
					) else (
						add_range p1 Range_empty new_point;
						add_range new_point Range_queued p2;
						true
					)
				)
			)
			| _ -> (
				(* There were no valid queues *)
				print "extend_small_thresh_queue found no ranges of sufficient patheticosity\n";
				false
			)
		) in

		(* This queues a new job in the middle of the largest Range_empty *)
		let split_largest_range batch_length ranges = (
			print "batch length: %d\n" batch_length;
			let rec largest_range_size so_far = function
				| {range_next = None} -> so_far
				| {range_frame = a; range_next = Some {range_type = Range_empty; range_right = ({range_frame = b} as next)}} -> largest_range_size (max so_far (b - a)) next
				| {range_next = Some {range_right = next}} -> largest_range_size so_far next
			in
			let largest_range = largest_range_size 0 ranges in
			print "split_largest_range found the biggest empty range to be %d frames long\n" largest_range;
			if largest_range < batch_length + o.o_split_frame_length then (
				print " but that's not long enough! (%d)\n" (batch_length + o.o_split_frame_length);
				false
			) else (
				let rec split_ranges_of_size stuff_happened p1 = (match p1 with
					| {range_next = None} -> stuff_happened
					| {range_frame = a; range_next = Some {range_type = Range_empty; range_right = ({range_frame = b} as p2)}} when b - a = largest_range -> (
						(* Found one! *)
						let range_frames = b - a in
						(* First check to see if the range can only be split into two parts *)
						(* If the length of the starting range is less than 2 * batch_length + split_length, then there is no room for a third part, no matter where the second part starts *)
						let begin_start = a + (
							if range_frames <= 2 * batch_length + o.o_split_frame_length then (
								(range_frames - o.o_split_frame_length) / 2
							) else if range_frames <= 3 * batch_length + 2 * o.o_split_frame_length then (
								batch_length
							) else (
								(range_frames - batch_length) / 2 - o.o_split_frame_length
							)
						) in
						let p_begin = check_nth_frame begin_start (pred b) (i.i_get_fast_frame_y begin_start) 0.0 0 1 1 in
						Printf.fprintf out_stats_handle "#SPLIT %d %f\n" p_begin.range_frame p_begin.range_thresh;
						print "split_largest_range found start I frame %d in (%d,%d)\n" p_begin.range_frame a b;
						let i_frame_rel_to_range = p_begin.range_frame - a in
						if i_frame_rel_to_range + batch_length + o.o_split_frame_length > range_frames then (
							(* There's only room for that one split *)
							print " there's only room for two in this here range\n";
							match p2 with
							| {range_delfirst = true; range_next = Some {range_right = next}} -> (
								print "  split_largest_range wants to split to Empty :: Queued, but the next point is incorrect; throwing out\n";
								split_ranges_of_size stuff_happened next
							)
							| _ -> (
								add_range p1 Range_empty p_begin;
								add_range p_begin Range_queued p2;
								true
							)
						) else (
							(* Get a second split point and queue what's in the middle *)
							let end_start = p_begin.range_frame + batch_length in
							let p_end = check_nth_frame end_start (pred b) (i.i_get_fast_frame_y end_start) 0.0 0 1 1 in
							Printf.fprintf out_stats_handle "#SPLIT %d %f\n" p_end.range_frame p_end.range_thresh;
							add_range p1 Range_empty p_begin;
							add_range p_begin Range_queued p_end;
							add_range p_end Range_empty p2;
							true
						)
					)
					| {range_next = Some {range_right = next}} -> split_ranges_of_size stuff_happened next
				) in
				split_ranges_of_size false ranges
			)
		) in

		(* This should also not be dependent on batch_now_unsafe, since it does not add any split points *)
		let queue_something_empty ranges = (
			let is_valid_start = function
				| {range_next = Some {range_type = Range_empty}} -> true
				| _ -> false
			in
			match find_largest_valid_thresh is_valid_start range_list with
			| Some ({range_next = Some {range_right = p2}} as p1) -> (
				add_range p1 Range_queued p2;
				true
			)
			| _ -> false
		) in
(* OLD
		let max_queues_after_full_or_working = Array.length o.o_agents_1 in
		let max_queues_split = min (max 1 ((max_queues_after_full_or_working - 2) / 2 + 2)) max_queues_after_full_or_working in
		let max_queues_other = 1 in
*)
		(* This part is extremely hacky since the agents are now dynamic *)
		(* It was already pretty hacky before, but now it's got extra hacks! *)
		let max_queues_after_full_or_working = max 6 (Array.fold_left (fun so_far {ac_agents = (x,_)} -> so_far + x) 0 o.o_agents) in
		let max_queues_extend = min (2 * (max_queues_after_full_or_working - 3) / 3 + 3) max_queues_after_full_or_working in
		let max_queues_split = min ((max_queues_after_full_or_working - 2) / 3 + 2) max_queues_after_full_or_working in
		let max_queues_other = 1 in


		Mutex.lock range_list_mutex;
		print_mutex "locked range_list_mutex\n";
		print "Split points at %d,%d,%d,%d\n" max_queues_after_full_or_working max_queues_extend max_queues_split max_queues_other;

		ignore (queue_beginning range_list); (* It doesn't really matter if this goes through, so ignore it *)
		
		let rec queue_pass () = (
			(* All these functions assume that range_list_mutex is locked *)
			let rec wait_on_full () = (
				print "Splitter: wait_on_full\n";

				print_mutex "broadcasting range_list_empty_condition\n";
				Condition.broadcast range_list_empty_condition;

(*				print_ranges "Waiting on full with:\n";*)
				print_everything ();
				print_mutex "waiting on range_list_full_condition; unlocking range_list_mutex\n";
				Condition.wait range_list_full_condition range_list_mutex;
				print_mutex "finished waiting on range_list_full_condition; locked range_list_mutex\n";
				check_if_all_done_or_empty ()
			)
			and broadcast_empty () = (
				print "Splitter: broadcast empty\n";
				Condition.broadcast range_list_empty_condition;
				check_if_all_done_or_empty ()
			)
			and check_if_all_done_or_empty () = (
				print "Splitter: check_if_all_done_or_empty\n";
(*				print_ranges "In the splitter:\n";*)
				print_everything ();

				print_mutex "unlocking range_list_mutex and yielding\n";
				Mutex.unlock range_list_mutex;
(*
				print_mutex "yielding!\n";
				Thread.yield ();
				print_mutex "stopped yielding\n";
*)

				print_mutex "sleeping for 1/10 second\n";
				Thread.delay 0.1;

				Mutex.lock range_list_mutex;
				print_mutex "locked range_list_mutex after yielding\n";

				let ((e,f,q,w),(es,fs,qs,ws)) = count_all_ranges_larger o.o_rethresh ((0,0,0,0),(0,0,0,0)) range_list in

				print " e  %d\n" e ;
				print " f  %d\n" f ;
				print " q  %d\n" q ;
				print " w  %d\n" w ;
				print " es %d\n" es;
				print " fs %d\n" fs;
				print " qs %d\n" qs;
				print " ws %d\n" ws;

				if e + q + w + es + qs + ws = 0 then (
					(* No more; exit *)
				) else if e = 0 && es = 0 then (
					print "Splitter: wait for something to get emptied\n";
					wait_on_full ()
				) else (
					let batch_length = batch_now_unsafe () in
					print "Splitter: current batch length is %d\n" batch_length;
					try_queue_after_full_or_working batch_length q
				)
			)
			and try_queue_after_full_or_working batch_length q = (
				print "Splitter: try_queue_after_full_or_working\n";
				if q >= max_queues_after_full_or_working then (
					(* Too many *)
					wait_on_full ()
				) else if queue_after_full_or_working batch_length range_list then (
					(* Splitting after full did something; continue *)
					broadcast_empty ()
				) else (
					(* Keep going *)
					try_extend_small_thresh_queue batch_length q
				)
			)
			and try_extend_small_thresh_queue batch_length q = (
				print "Splitter: try_extend_small_thresh_queue\n";
				if q >= max_queues_extend then (
					(* Too many *)
					wait_on_full ()
				) else if extend_small_thresh_queue batch_length range_list then (
					(* Splitting largest range did something *)
					broadcast_empty ()
				) else (
					(* Keep going *)
					try_split_largest_range batch_length q
				)
			)
			and try_split_largest_range batch_length q = (
				print "Splitter: try_split_largest_range\n";
				if q >= max_queues_split then (
					wait_on_full ()
				) else if split_largest_range batch_length range_list then (
					(* Splitting largest range did something *)
					broadcast_empty ()
				) else (
					(* Keep going *)
					try_queue_something_empty batch_length q
				)
			)
			and try_queue_something_empty batch_length q = (
				print "Splitter: try_queue_something_empty\n";
				if q >= max_queues_other then (
					wait_on_full ()
				) else if queue_something_empty range_list then (
					broadcast_empty ()
				) else (
					(* Nothing to do *)
					wait_on_full ()
				)
			)
			in
			check_if_all_done_or_empty ()
		) in
		queue_pass ();
		
		print_mutex "broadcasting range_list_empty_condition and unlocking range_list_mutex\n";
		Condition.broadcast range_list_empty_condition;
		Mutex.unlock range_list_mutex;

	) in


	(**********)
	(* DISHER *)
	(**********)
	let disher_guts (name,(ip,port),n) = (
		let print = (fun a -> print ~name:(name ^ string_of_int n ^ " ") a) in
		let print_log = (fun a -> Controllerinclude.print ~screen:false ~name:(name ^ " " ^ string_of_int n ^ " ") a) in
		let print_mutex = (fun a -> Controllerinclude.print ~screen:false ~name:(String.uppercase name ^ " MUTEX ") a) in
		let update = update_status (ip,port) n in
		let fps_start () = fps_disher_start (ip,port) n in
		let fps_stop = fps_disher_stop (ip,port) n in
		print_log "starting on %s (id %d)\n" ip (Thread.id (Thread.self ()));
		
		(* If all the jobs are full and the splitter is done, exit *)
		let rec maybe_more_jobs_unsafe = function
			| {range_next = Some {range_type = Range_full _; range_right = next}} -> maybe_more_jobs_unsafe next
			| {range_next = None} -> false
			| _ -> true
		in
		let check_for_more_jobs () = (
			Mutex.lock range_list_mutex;
			print_mutex "locked range_list_mutex to check for more jobs\n";
			let a = maybe_more_jobs_unsafe range_list in
(*
			if a then (
				print_mutex "broadcasting range_list_empty_condition, since more jobs exist\n";
				Condition.broadcast range_list_empty_condition;
			);
*)
			print_mutex "unlocking range_list_mutex\n";
			Mutex.unlock range_list_mutex;
			a
		) in
		
		let timeout = 5 in
		
		let rec attempt_connection () = (
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
(*
			print "RECV TIMEOUT: %f\n" (Unix.getsockopt_float sock Unix.SO_RCVTIMEO);
			Unix.setsockopt_float sock Unix.SO_RCVTIMEO 20.0;
*)
			let send = send sock in
			let recv () = recv sock in
			if not (check_for_more_jobs ()) then (
				print_log "attempt_connect sees no jobs left; DEAD\n";
				()
			) else (
				try (
(*print "testing port %d\n" port;*)
					Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string ip, port));
(*print "connected\n";*)
					send "HELO" "";
(*print "correct\n";*)
(*					update ("Connected to " ^ ip ^ ":" ^ string_of_int port);*)
					update (S_Connected (ip,port));

					(* Compression figurer *)
					send "SCOM" supported_compressions;
					let compress = (match recv () with
						| ("SCO1", x) -> (
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
					
					(* Ignore the second pass compression settings *)
					(match recv () with
						| ("SCO2", x) -> ()
						| x -> raise (unexpected_packet x)
					);

					check_out_job sock send recv port compress
				) with
				| e -> (
(*
					(match e with
						| Unix.Unix_error (x,y,z) -> print "Unix error (%d=%S,%s,%s)\n" (Obj.magic x) (Unix.error_message x) y z
						| _ -> print "%s\n" (Printexc.to_string e);
					);
*)
					Unix.close sock;
(*					update "Disconnected";*)
					update S_Disconnected;
					Unix.sleep timeout;
					attempt_connection ()
				)
			)


		) and check_out_job sock send recv port compress = (

(* RANGE_LIST *)
			let rec check_out_best_queued_range_after_full ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = (None | Some {range_type = Range_full _}); range_next = Some {range_type = Range_queued}} -> true
					| _ -> false
				in
				match find_largest_valid_thresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			(* All of the _before_small_queued functions return a large-threshold queued range before a small-threshold queued range *)
			(* This situation is given precedence over the others in order to start on the parts of the movie which need to be done serially *)
			let check_out_best_queued_range_after_empty_before_small_queued ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_empty}; range_next = Some {range_type = Range_queued; range_right = {range_thresh = next_thresh}}} when next_thresh < o.o_rethresh -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			let check_out_best_queued_range_after_working_before_small_queued ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_working _}; range_next = Some {range_type = Range_queued; range_right = {range_thresh = next_thresh}}} when next_thresh < o.o_rethresh -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			let check_out_best_queued_range_after_queued_before_small_queued ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_queued}; range_next = Some {range_type = Range_queued; range_right = {range_thresh = next_thresh}}} when next_thresh < o.o_rethresh -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			let check_out_best_queued_range_after_empty ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_empty}; range_next = Some {range_type = Range_queued}} -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			(* Might as well do it now. If the next worker finished it first, it will be the same. *)
			(* If this worker finishes first, it may make the next worker DELFIRST, *)
			(* but that would have to happen anyway *)
			let check_out_best_queued_range_after_working ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_working _}; range_next = Some {range_type = Range_queued}} -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			let check_out_best_queued_range_after_queued ranges = (
				let is_valid_start = function
					| {range_next = Some {range_right = {range_delfirst = true}}} -> false
					| {range_prev = Some {range_type = Range_queued}; range_next = Some {range_type = Range_queued}} -> true
					| _ -> false
				in
				match find_largest_valid_thresh ~above:o.o_rethresh is_valid_start ranges with
				| Some {range_next = next_perhaps} -> next_perhaps
				| None -> None
			) in

			Mutex.lock range_list_mutex;
			print_mutex "locked range_list_mutex to check out a job\n";

			let try_in_this_order = [
				(check_out_best_queued_range_after_full                       , "check_out_best_queued_range_after_full");
				(check_out_best_queued_range_after_empty_before_small_queued  , "check_out_best_queued_range_after_empty_before_small_queued");
				(check_out_best_queued_range_after_working_before_small_queued, "check_out_best_queued_range_after_working_before_small_queued");
				(check_out_best_queued_range_after_queued_before_small_queued , "check_out_best_queued_range_after_queued_before_small_queued");
				(check_out_best_queued_range_after_empty                      , "check_out_best_queued_range_after_empty");
				(check_out_best_queued_range_after_working                    , "check_out_best_queued_range_after_working");
				(check_out_best_queued_range_after_queued                     , "check_out_best_queued_range_after_queued");
			] in

			let rec check_out_job_like_this = function
				| (f,n) :: tl -> (
					match f range_list with
					| Some range -> (
						print_log "%s found range %d-%d\n" n range.range_left.range_frame range.range_right.range_frame;
						Some range
					)
					| None -> (
						print_log "%s found no suitable ranges\n" n;
						check_out_job_like_this tl
					)
				)
				| [] -> (
					print_log "no ranges found\n";
					if maybe_more_jobs_unsafe range_list then (
(*						update "Waiting for job";*)
						update S_Waiting;
						print_mutex "waiting on range_list_empty_condition; unlocking range_list_mutex\n";
						Condition.wait range_list_empty_condition range_list_mutex;
						check_out_job_like_this try_in_this_order
					) else (
						print_log "found no jobs left. DEAD\n";
						Condition.broadcast range_list_empty_condition;
						None
					)
				)
			in
			let a_job_perhaps = check_out_job_like_this try_in_this_order in

			(* Expand the range and set it to working *)
			let new_job_perhaps = match a_job_perhaps with
				| None -> None (* Ignore *)
				| Some range -> (
					let batch = batch_now_unsafe () in
					let rec combine_queued = function
						| {range_left = ({range_frame = a} as left); range_right = {range_next = Some {range_type = Range_queued; range_right = ({range_frame = b; range_delfirst = false} as right)}}} when b - a <= batch + o.o_split_frame_length -> (
							print_log " combining range with next queued range; range is now %d - %d (%d frames)\n" a b (b - a);
							combine_queued (add_range_and_return left Range_queued right)
						)
						| x -> x
					in
					let new_range = combine_queued range in
					Some (add_range_and_return range.range_left (Range_working name) range.range_right)
				)
			in
			print_mutex "unlocking range_list_mutex\n";
			Mutex.unlock range_list_mutex;
			
			match new_job_perhaps with
			| None -> (
				print_log "check_out_job sees no jobs left; disconnect\n";
				close_connection sock send recv port
			)
			| Some range -> (
				print_log "check_out_job got frames %d - %d\n" range.range_left.range_frame range.range_right.range_frame;

				print_mutex "broadcasting range_list_full_condition\n";
				Condition.broadcast range_list_full_condition;

				process_job sock send recv port compress range
			)

		) and process_job sock send recv port compress range = (
			let is_last_range = (range.range_right.range_frame = i.i_num_frames) in
			let a = range.range_left.range_frame in                                                (* The frame to start encoding from (and therefore the start frame of the output array) *)
			let b = if is_last_range then i.i_num_frames - 1 else range.range_right.range_frame in (* The frame to end encoding *)
			let first_output_frame = a in                                                          (* The first frame that should be outputted to the range list *)
			let last_output_frame = if is_last_range then b else b - 1 in                          (* The last frame that should be outputted to the range list (equal to the last frame to be encodod - why encode it otherwise?) *)
			let first_frame = max 0 (a - o.o_preseek) in                                           (* The first frame to get from avs2yuv *)
			let num_frames = b - a + 1 in

(*			update (Printf.sprintf "Checked out frames %d-%d" a b);*)
			update (S_Checked_out (a,b));

			(* Reset the range to "queued" *)

			let reset_range_unsafe () = (
				add_range range.range_left Range_queued range.range_right
			) in

			(* More better *)
			(* This version resets the range, combines the empty range with the previous and next ranges, *)
			(* sets DELFIRST, and removes part of the next range, if it's Full *)
			(* WARNING: DO NOT SET DELFIRST IF THERE IS A CONNECTION ISSUE *)
			let remove_range_unsafe () = (
				print_log "RESET RANGE\n";
  		
				range.range_left.range_delfirst <- false; (* Reset beginning DELFIRST *)
				
				(* This function fixes up the right side of the range so that DELFIRST gets set properly *)
				(
					match range.range_right.range_next with
					| (Some {range_type = Range_working _}) -> (
						(* DELFIRST OK *)
						print_log "next is working; set to DELFIRST\n";
						Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (range.range_right.range_frame - 1);
						range.range_right.range_delfirst <- true;
					)
					| (Some {range_type = Range_full full_array; range_right = point_after_full}) -> (
						(* Take a chunk out of the next full range OK *)
						print_log "next is full; take a chunk out of it\n";
						let len = Array.length full_array in
						let rec find_first_i_frame now = (
							if now > len - 2 then (
								None
							) else (
								match full_array.(now) with
								| Frame_I _ -> Some now
								| _ -> find_first_i_frame (succ now)
							)
						) in
						let i_frame_option = find_first_i_frame 0 in
						match (i_frame_option, point_after_full.range_next) with
						| (Some x, _) -> (
							(* OK *)
							print_log "  chunk removed; fixing ranges\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (range.range_right.range_frame + x);
							let new_last_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = range.range_right.range_frame + x + 1; range_thresh = infinity} in
							add_range range.range_right Range_empty new_last_point;
							add_range new_last_point (Range_full (Array.sub full_array (x + 1) (len - x - 1))) point_after_full;
						)
						| (None, Some {range_type = Range_empty; range_right = very_far_away}) -> (
							(* OK *)
							print_log "  removing full range and extending next empty range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
							add_range range.range_right Range_empty very_far_away;
						)
						| (None, Some {range_type = Range_queued; range_right = very_far_away}) -> (
							(* OK *)
							print_log "  removing full range and extending next queued range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
							add_range range.range_right Range_queued very_far_away;
						)
						| (None, Some {range_type = Range_working _}) -> (
							(* OK *)
							print_log "  removing full range and putting in an empty range, and setting point after that to DELFIRST\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
							add_range range.range_right Range_empty point_after_full;
							point_after_full.range_delfirst <- true;
						)
						| (None, _) -> (
							(* OK *)
							print_log "  removing full range and putting in an empty range from %d to %d\n" range.range_right.range_frame point_after_full.range_frame;
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
							add_range range.range_right Range_empty point_after_full;
						)
					)
					| None -> (
						(* OK *)
						print_log "last range; nothing to do\n";
						Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (range.range_right.range_frame - 1);
					)
					| _ -> (
						(* Don't bother OK *)
						Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (range.range_right.range_frame - 1);
					)
				);
				
				print_log "last_range          = %B\n" is_last_range;
				print_log "a                   = %d\n" a;
				print_log "b                   = %d\n" b;
				print_log "first_output_frame  = %d\n" first_output_frame;
				print_log "last_output_frame   = %d\n" last_output_frame;
				print_log "num_frames          = %d\n" num_frames;
				print_log "\n";
  		
				let new_range = match (range.range_left.range_prev, range.range_right.range_next) with
					| (Some {range_type = Range_empty; range_left = left}, Some {range_type = Range_empty; range_right = right}) -> (
						(* OK *)
						print_log "combining three empty ranges\n";
						add_range_and_return left Range_empty right;
					)
					| (Some {range_type = Range_empty; range_left = left}, _) -> (
						(* OK *)
						print_log "combining previous empty range\n";
						add_range_and_return left Range_empty range.range_right;
					)
					| (_, Some {range_type = Range_empty; range_right = right}) -> (
						(* OK *)
						print_log "combining next empty range\n";
						add_range_and_return range.range_left Range_empty right;
					)
					(* Set the ranges if they are queued! *)
					(* This is to fix a DELFIRST-like condition when the range after it is queued but there are no I frames in the current range *)
					(* In this case, the range will be quickly queued and re-encoded, then return with the same result *)
					| (Some {range_type = Range_queued; range_left = left}, _) when range.range_right.range_frame - left.range_frame <= o.o_batch_length - o.o_split_frame_length -> (
						(* Merge the range with the previous queued range if it will result in a queued range which is smaller than the max *)
						print_log "combining previous queued range\n";
						add_range_and_return left Range_queued range.range_right;
					)
					| (_, Some {range_type = Range_queued; range_right = right}) when right.range_frame - range.range_left.range_frame <= o.o_batch_length - o.o_split_frame_length -> (
						print_log "combining next queued range\n";
						add_range_and_return range.range_left Range_queued right;
					)
					| (Some {range_type = Range_queued; range_left = left}, Some {range_type = Range_queued; range_right = right}) -> (
						(* Just set the whole thing to empty *)
						print_log "resetting three queued ranges to empty\n";
						add_range_and_return left Range_empty right;
					)
					| (Some {range_type = Range_queued; range_left = left}, _) -> (
						print_log "resetting previous queued range to empty and combining\n";
						add_range_and_return left Range_empty range.range_right;
					)
					| (_, Some {range_type = Range_queued; range_right = right}) -> (
						print_log "resetting next queued range to empty and combining\n";
						add_range_and_return range.range_left Range_empty right;
					)
					| _ -> (
						(* OK *)
						print_log "just hack in an empty range\n";
						add_range_and_return range.range_left Range_empty range.range_right;
					)
				in
				(* Now normalize the frames, since multiple consecutive empty ranges may have been made *)
				(* Although this is normally not a problem, since we are here something must have gone wrong, so reset the whole area *)
				match new_range with
				| {range_type = Range_empty; range_left = {range_prev = Some {range_type = Range_empty; range_left = left}}; range_right = {range_next = Some {range_type = Range_empty; range_right = right}}} -> add_range left Range_empty right
				| {range_type = Range_empty; range_right = {range_next = Some {range_type = Range_empty; range_right = right}}} -> add_range new_range.range_left Range_empty right
				| {range_type = Range_empty; range_left = {range_prev = Some {range_type = Range_empty; range_left = left}}} -> add_range left Range_empty new_range.range_right
				| _ -> ()
			) in


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

				fps_start ();

				(
					match recv () with
					| ("ENCD","agent") -> (
						(* Don't need to do anything; just wait for the agent to send everything back *)
						print_log "agent-based encoding; waiting for agent to finish\n";

(*						update (Printf.sprintf "Doing frames %d-%d (%d)" a b (b - a + 1));*)
						update (S_Doing (a,b));

						(*
							This part is here just so execution stops here instead of after the frames are set to No_frame.
							If the splitter gets ahold of frame_array after the frames have been set to No_frame, it will
							assume that the frames are actually not rendered, and will add them to the next range.
							This next line does not eliminate that possibility, unfortunately; it just minimizes it
						*)
						let rec grab_output () = (
							(match Unix.select [sock] [] [] (30.0) with
								| ([x],[],[]) -> (
(*									print "Hey! Something's here!\n";*)
									(match recv () with
										| ("INDY","") -> (
(*											print "got heartbeat signal\n";*)
											grab_output ()
										)
										| ("DONE","") -> () (* Done! *)
										| x -> raise (unexpected_packet x)
									)
								)
								| _ -> (
(*									update "Dead?";*)
									update S_Dead;
									raise Timeout
								)
							);
						) in
						grab_output ()
					)
					| ("ENCD","controller") -> (
	
(*						update (Printf.sprintf "Sending frames %d-%d (%d)" a b (b - a + 1));*)
						update (S_Sending (a,b));

						let get = Unix.open_process_in (Printf.sprintf "\"\"%s\" -raw -seek %d -frames %d -o - \"%s\" 2>%s\"" i.i_avs2yuv (first_frame + o.o_seek) (b - first_frame + 1) o.o_input_avs dev_null) in
						if first_frame <> a then (
							let ignore_bytes = i.i_bytes_per_frame * (a - first_frame) in
							let str = String.create ignore_bytes in
							really_input get str 0 ignore_bytes
						);
			
						compress num_frames get sock;

						ignore (Unix.close_process_in get);
				
						send "EOF!" "";
					)
					| x -> raise (unexpected_packet x)
				);

				fps_stop (b - a + 1);

				(* Receive output *)
				let temp_stats_array = Array.make (b - a + 1) No_frame in
				let rec recv_stats last_frame_i = (
					match recv () with
					| ("STAT", x) -> (
						(* A stat line *)
						match stats_line_of_string x with
						| Some s -> (
							(* It's a frame! *)
							s.stats_in <- s.stats_in + a;
							s.stats_out <- s.stats_out + a;
							if s.stats_type = Stats_frame_I then (
								temp_stats_array.(s.stats_out - a) <- Frame_I (string_of_stats_line s);
								recv_stats ();
							) else (
								temp_stats_array.(s.stats_out - a) <- Frame_notI (string_of_stats_line s);
								recv_stats ();
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
								print_log "WARNING: Don't know how to handle %S\n" x;
								recv_stats ();
							)
						)
					)
					| ("EOF!","") -> (print_log "done recieving stats\n")
					| x -> raise (unexpected_packet x)
				) in
				recv_stats ();
				
				print_log "DONE writing to temp stats array!\n";
(*				update (Printf.sprintf "Done with frames %d-%d" a b);*)
				update (S_Done (a,b));
				
				(* Now all the frames are in temp_stats_array in display order *)
				
				(* Update the max GOP length (how's that for a descriptive comment?) *)
				update_max_gop_length temp_stats_array;
(*				add_error (Printf.sprintf "Max GOP length: %d" !max_gop_length_so_far_ref);*)
				
				(* Check for the last I frame and if there are any No_frames *)
				let rec find_last_i_frame found_last_i_frame on = match temp_stats_array.(on) with
					| Frame_I _ -> ((*print "%d is I\n" on;*) if on = 0 then (false, found_last_i_frame) else if found_last_i_frame = 0 then find_last_i_frame on (pred on) else find_last_i_frame found_last_i_frame (pred on))
					| No_frame  -> ((*print "%d is NOFRAME!\n" on;*) (true, found_last_i_frame))
					| _ -> ((*print "%d is something else\n" on;*) if on = 0 then (false, 0) else find_last_i_frame found_last_i_frame (pred on))
				in
				(* Find the first I frame in an array *)
				let rec find_first_i_frame a now = (
					if now >= Array.length a then (
						Array.length a
					) else (
						match a.(now) with
						| Frame_I _ -> now
						| _ -> find_first_i_frame a (succ now)
					)
				) in

				(* Starts from the end *)
				let find_first_last_none () = (
					let rec helper first last now = (
						if now <= 0 then (
							Some (first, last)
						) else (
							match temp_stats_array.(now) with
							| No_frame -> None
							| Frame_I x -> helper now (max last now) (pred now)
							| Frame_notI x -> helper first last (pred now)
						)
					) in
					helper 0 0 (Array.length temp_stats_array - 1)
				) in

				(* Given an array of frames, this will find the first I frame which is not at the beginning of a GOP with the maximum GOP length *)
				(* It will start searching at the second frame, and therefore will not match the first GOP *)
				let find_first_i_frame_not_before_max_length_gop this_array = (
					let max_gop_length_so_far = !max_gop_length_so_far_ref in
					let rec find_next_i_frame now = (
						if now >= Array.length this_array then (
							now
						) else (
							match this_array.(now) with
							| Frame_I _ -> now
							| _ -> find_next_i_frame (succ now)
						)
					) in
					let rec helper last_i_frame now = (
						if now >= Array.length this_array then (
							None
						) else (
							match this_array.(now) with
							| No_frame -> None
							| Frame_I _ when now - last_i_frame = max_gop_length_so_far -> (
								(* Nope. Try again *)
								helper now (succ now)
							)
							| Frame_I _ -> (
								(* YAY! *)
								Some last_i_frame
							)
							| Frame_notI _ -> (
								helper last_i_frame (succ now)
							)
						)
					) in
					let first = find_next_i_frame 1 in
					(* If the first frame is not frame number max_gop_length_so_far, then that's the answer *)
					if first < Array.length this_array && first <> max_gop_length_so_far then (
						Some first
					) else (
						helper first (succ first)
					)
				) in
				let find_last_i_frame this_array = (
					let rec helper now = (
						if now <= 0 then (
							None
						) else (
							match this_array.(now) with
							| No_frame -> None
							| Frame_I _ -> Some now
							| Frame_notI _ -> helper (pred now)
						)
					) in
					helper (Array.length this_array - 1)
				) in
				let (first_i_frame_perhaps, last_i_frame_perhaps) = (find_first_i_frame_not_before_max_length_gop temp_stats_array, find_last_i_frame temp_stats_array) in

(match (first_i_frame_perhaps, last_i_frame_perhaps) with
	| (Some x, Some y) -> print_log "found usable I frames from %d to %d\n" x y
	| (None, None) -> print_log "found no usable I frames!\n"
	| (Some x, None) | (None, Some x) -> print_log "WHAT! Only one I frame at %d!\n" x
);

				let found_some_i_frames = find_first_last_none () in
				let start_offset = if a = 0 then 0 else 1 in

				(* PUT BACK RANGE *)
				(* PUT BACK RANGE *)
				(* PUT BACK RANGE *)
(*				let put_range_back_unsafe first_i_frame last_i_frame = ( *)
				let put_range_back_unsafe copy_from copy_to = (
					print_log "PUT BACK RANGE\n";
  			
					let first_index_in_range = first_output_frame - a in
					let last_index_in_range = last_output_frame - a in
					let first_index_to_copy = copy_from in
					let last_index_to_copy = copy_to in
  			
					range.range_left.range_delfirst <- false; (* Reset DELFIRST *)
					
					(* This function normalizes the working range with the previous range *)
					let (prev_range_perhaps, first_valid_point) = (
						match (range.range_left.range_prev, first_index_to_copy = first_index_in_range) with
						| (Some ({range_type = Range_full _} as r), true) -> (
							(* OK *)
							print_log "merging proper start with previous full range\n";
							(Some r, range.range_left)
						)
						| (_, true) -> (
							(* OK *)
							print_log "appending range to some other range type\n";
							(None, range.range_left)
						)
						| (Some {range_type = Range_empty; range_left = p1}, false) -> (
							(* OK *)
							print_log "adding first frames to previous empty range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
							add_range p1 Range_empty new_point;
							(None, new_point)
						)
						| (Some {range_type = Range_queued; range_left = p1}, false) -> (
							(* OK *)
							print_log "adding first frames to previous queued range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
							add_range p1 Range_queued new_point;
							(None, new_point)
						)
						| (_, false) -> (
							(* OK *)
							print_log "adding empty range after some sort of other range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
							add_range range.range_left Range_empty new_point;
							(None, new_point)
						)
					) in
					(* This function normalizes the working range with the next range *)
					let (next_range_perhaps, last_valid_point) = (
						match (range.range_right.range_next, last_index_to_copy = last_index_in_range) with
						| (Some ({range_type = Range_full _} as r), true) -> (
							(* OK *)
							print_log "merging proper end with next full range\n";
							(Some r, range.range_right)
						)
						| (_, true) -> (
							(* OK *)
							print_log "prepending range before some other range type\n";
							(None, range.range_right)
						)
						| (Some {range_type = Range_empty; range_right = p2}, false) -> (
							(* OK *)
							print_log "adding last frames to next empty range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
							add_range new_point Range_empty p2;
							(None, new_point)
						)
						| (Some {range_type = Range_queued; range_right = p2}, false) -> (
							(* OK *)
							print_log "adding last frames to next queued range\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
							add_range new_point Range_queued p2;
							(None, new_point)
						)
						| (Some {range_type = Range_working _}, false) -> (
							(* DELFIRST OK *)
							print_log "next is working; set to DELFIRST\n";
							Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
							let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
							add_range new_point Range_empty range.range_right;
							range.range_right.range_delfirst <- true;
							(None, new_point)
						)
						| (Some {range_type = Range_full full_array; range_right = point_after_full}, false) -> (
							(* Take a chunk out of the next full range OK *)
							print_log "next is full; take a chunk out of it\n";
							let new_first_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
							let len = Array.length full_array in
  			
							let rec find_first_i_frame now = (
								if now > len - 2 then (
									None
								) else (
									match full_array.(now) with
									| Frame_I _ -> Some now
									| _ -> find_first_i_frame (succ now)
								)
							) in
  			
							let i_frame_option = find_first_i_frame_not_before_max_length_gop full_array in
(*			
							(match olde_i_frame with
								| None ->   print_log "  No first I frame found\n"
								| Some x -> print_log "  First I frame at %d\n" x
							);
*)			
							(match i_frame_option with
								| None ->   print_log "  No REAL I frame found\n"
								| Some x -> print_log "  REAL I frame at  %d\n" x
							);
							match (i_frame_option, point_after_full.range_next) with
							| (Some x, _) -> (
								(* OK *)
								(* Fixed up the chunkage (x -> x - 1) *)
								print_log "  chunk removed; fixing ranges\n";
								Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (range.range_right.range_frame + x - 1);
								let new_last_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = range.range_right.range_frame + x; range_thresh = infinity} in
								add_range new_first_point Range_empty new_last_point;
								add_range new_last_point (Range_full (Array.sub full_array (x) (len - x))) point_after_full;
								(None, new_first_point)
							)
							| (None, Some {range_type = Range_empty; range_right = very_far_away}) -> (
								(* OK *)
								print_log "  removing full range and extending next empty range\n";
								Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
								add_range new_first_point Range_empty very_far_away;
								(None, new_first_point)
							)
							| (None, Some {range_type = Range_queued; range_right = very_far_away}) -> (
								(* OK *)
								print_log "  removing full range and extending next queued range\n";
								Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
								add_range new_first_point Range_queued very_far_away;
								(None, new_first_point)
							)
							| (None, Some {range_type = Range_working _}) -> (
								(* OK *)
								print_log "  removing full range and putting in an empty range, and setting point after that to DELFIRST\n";
								Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
								add_range new_first_point Range_empty point_after_full;
								point_after_full.range_delfirst <- true;
								(None, new_first_point)
							)
							| (None, _) -> (
								(* OK *)
								print_log "  removing full range and putting in an empty range from %d to %d\n" new_first_point.range_frame point_after_full.range_frame;
								Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
								add_range new_first_point Range_empty point_after_full;
								(None, new_first_point)
							)
						)
						| (None, false) -> (
							(* OK *)
							(* Last range; last output should be last frame *)
							(None, range.range_right)
						)
					) in
					
					print_log "last_range          = %B\n" is_last_range;
					print_log "a                   = %d\n" a;
					print_log "b                   = %d\n" b;
					print_log "first_output_frame  = %d\n" first_output_frame;
					print_log "last_output_frame   = %d\n" last_output_frame;
					print_log "num_frames          = %d\n" num_frames;
					print_log "\n";
					print_log "first_index_range   = %d\n" first_index_in_range;
					print_log "last_index_range    = %d\n" last_index_in_range;
					print_log "copy_from           = %d\n" copy_from;
					print_log "copy_to             = %d\n" copy_to;
					print_log "first_index_copy    = %d\n" first_index_to_copy;
					print_log "last_index_copy     = %d\n" last_index_to_copy;
					print_log "\n";
(*
					let last_index_to_copy_2 = (if use_all then last_index_in_range else last_index_to_copy) in
					print_log "modified last index = %d %s\n" last_index_to_copy_2 (if last_index_to_copy = last_index_to_copy_2 then "SAME" else "DIFFERENT!");
*)
				
					(* Output the array to the stats file *)
					partial_array_iter (function
						| No_frame -> ()
						| (Frame_I x | Frame_notI x) -> Printf.fprintf out_stats_handle "%s\n" x
					) temp_stats_array first_index_to_copy (last_index_to_copy - first_index_to_copy + (if is_last_range then 1 else 2));
				
					match (prev_range_perhaps, next_range_perhaps) with
					| (Some {range_type = Range_full prev; range_left = p1}, Some {range_type = Range_full next; range_right = p2}) -> (
						(* OK *)
						print_log "merging three full ranges together\n";
						let a = Array.make (p2.range_frame - p1.range_frame) No_frame in
						Array.blit prev 0 a 0 (Array.length prev);
						Array.blit temp_stats_array first_index_to_copy a (first_valid_point.range_frame - p1.range_frame) (last_index_to_copy - first_index_to_copy + 1);
						Array.blit next 0 a (last_valid_point.range_frame - p1.range_frame) (Array.length next);
						add_range p1 (Range_full a) p2;
					)
					| (Some {range_type = Range_full prev; range_left = p1}, _) -> (
						(* OK *)
						print_log "merging range with its previous full range\n";
						let a = Array.make (last_valid_point.range_frame - p1.range_frame) No_frame in
						Array.blit prev 0 a 0 (Array.length prev);
						Array.blit temp_stats_array first_index_to_copy a (first_valid_point.range_frame - p1.range_frame) (last_index_to_copy - first_index_to_copy + 1);
						add_range p1 (Range_full a) last_valid_point;
					)
					| (_, Some {range_type = Range_full next; range_right = p2}) -> (
						(* OK *)
						print_log "merging range with its next full range\n";
						let a = Array.make (p2.range_frame - first_valid_point.range_frame) No_frame in
						Array.blit temp_stats_array first_index_to_copy a 0 (last_index_to_copy - first_index_to_copy + 1);
						Array.blit next 0 a (last_valid_point.range_frame - first_valid_point.range_frame) (Array.length next);
						add_range first_valid_point (Range_full a) p2;
					)
					| _ -> (
						(* OK *)
						print_log "no merging\n";
						let a = Array.sub temp_stats_array first_index_to_copy (last_index_to_copy - first_index_to_copy + 1) in
						add_range first_valid_point (Range_full a) last_valid_point;
					)
				) in


				let copy_from_perhaps = (
					match (first_i_frame_perhaps, range.range_left.range_delfirst) with
					| (None  , false) -> Some 0
					| (None  , true ) -> None
					| (Some x, false) -> Some 0
					| (Some x, true ) -> Some x
				) in
				let copy_to_perhaps = (
					match (last_i_frame_perhaps, is_last_range) with
					| (None  , false) -> None
					| (None  , true ) -> Some (Array.length temp_stats_array - 1)
					| (Some x, false) -> Some (x - 1)
					| (Some x, true ) -> Some (Array.length temp_stats_array - 1)
				) in

				(match (copy_from_perhaps, copy_to_perhaps) with
					| (Some c_from, Some c_to) -> (
						print_log "NOW copy frames %d - %d (%d frames total)\n" c_from c_to (Array.length temp_stats_array);

						Mutex.lock range_list_mutex;
						print_mutex "locked range_list_mutex to put back completed job\n";
						print_log "putting back (%d,%d)\n" range.range_left.range_frame range.range_right.range_frame;

						put_range_back_unsafe c_from c_to;

						flush out_stats_handle;
						print_mutex "broadcasting range_list_full_condition and unlocking range_list_mutex\n";
						Condition.broadcast range_list_full_condition;
						Mutex.unlock range_list_mutex;
						print_everything ();
					)
					| _ -> (
						(match (copy_from_perhaps, copy_to_perhaps) with
							| (Some x, None) -> print_log "NOW start on frame %d, but no end; DELETE\n" x
							| (None, Some y) -> print_log "NOW don't start, but end on frame %d; DELETE\n" y
							|       _        -> print_log "NOW don't copy anything; DELETE\n"
						);
						Mutex.lock range_list_mutex;
						print_mutex "locked range_list_mutex to reset the range\n";
						print_log "setting (%d,%d) back to empty\n" range.range_left.range_frame range.range_right.range_frame;
						remove_range_unsafe ();
						flush out_stats_handle;
						print_mutex "broadcasting range_list_full_condition and unlocking range_list_mutex\n";
						Condition.broadcast range_list_full_condition;
						Mutex.unlock range_list_mutex;
					)
				);


				(* Print the FPS *)
				print_log "trying another job\n";
				
				
				check_out_job sock send recv port compress
			) with
			| x -> (
				(match x with
					| Invalid_argument x -> print_log "process_job failed with \"Invalid_argument(%s)\"; putting job (%d,%d) back\n" x range.range_left.range_frame range.range_right.range_frame
					| Unix.Unix_error (x,y,z) -> print_log "process_job failed with (%S,%s,%s); putting job (%d,%d) back\n" (Unix.error_message x) y z range.range_left.range_frame range.range_right.range_frame
					| x -> print_log "process_job failed with %S; putting job (%d,%d) back\n" (Printexc.to_string x) range.range_left.range_frame range.range_right.range_frame
				);
				
(*				ignore (Unix.close_process_in get);*)
				
(*
				Mutex.lock splitter_list_mutex;
				List2.append splitter_list (a,b,r);
				Mutex.unlock splitter_list_mutex;
				Condition.broadcast splitter_list_condition;
				close_connection sock send recv port
*)

				Mutex.lock range_list_mutex;
				print_mutex "locked range_list_mutex to reset failed job\n";
				reset_range_unsafe ();
(*				Condition.broadcast range_list_full_condition;*)
				print_mutex "broadcasting range_list_full_condition and unlocking range_list_mutex\n";
				Condition.broadcast range_list_full_condition;
				Mutex.unlock range_list_mutex;

(*				update (Printf.sprintf "Frames %d-%d failed" a b);*)
				update (S_Failed (a,b));
				(match x with
					| Unix.Unix_error (x,y,z) -> add_error (Printf.sprintf "%s has failed with error %s on %s" name (unix_error_string x) y)
					| Timeout -> add_error (Printf.sprintf "%s timed out" name)
					| x -> add_error (Printf.sprintf "%s failed with \"%s\"" name (Printexc.to_string x))
				);

				close_connection sock send recv port

			)


		) and close_connection sock send recv port = (
			print_log "close connection\n";
(*			Unix.shutdown sock Unix.SHUTDOWN_ALL;*)
			(try
				Unix.close sock
			with
				_ -> ()
			);
(*			update "Disconnected";*)
			update S_Disconnected;
			attempt_connection ()
		) in
		
		attempt_connection ();

		print_log "exiting...\n";
(*		update "No jobs left; exited";*)
		update S_Exited;

	) in

	(*****************)
	(* END OF DISHER *)
	(*****************)

	(********)
	(* PING *)
	(********)
	let ping_guts () = (
		(* Send out a signal every so often *)
		let print = (fun a -> print ~screen:false ~name:"PING1 " a) in
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		Unix.setsockopt sock Unix.SO_BROADCAST true;
		let broadcast = match Sys.os_type with
			| "Win32" -> (fun x -> Unix.sendto sock x 0 (String.length x) [] (Unix.ADDR_INET (Obj.magic "\255\255\255\255", o.o_adhoc_agent)))
			| _ -> (fun x -> Unix.sendto sock x 0 (String.length x) [] (Unix.ADDR_INET (Unix.inet_addr_of_string "255.255.255.255", o.o_adhoc_agent)))
		in
		let sendthis = (
			let start = "CONT" in
			start ^ Digest.string start
		) in
		let rec keep_going_unsafe = function
			| {range_next = None} -> false
			| {range_next = Some {range_type = Range_full _; range_right = next}} -> keep_going_unsafe next
			| {range_next = Some _ } -> true
		in
		let rec ping_loop () = (
			Mutex.lock range_list_mutex;
			let keep_going = keep_going_unsafe range_list in
			Mutex.unlock range_list_mutex;
			print "Send ping? %B\n" keep_going;
			if keep_going then (
				(* Keep going! *)
				print "Pinging %S\n" sendthis;
				if broadcast sendthis < String.length sendthis then (
					print "Oops. Didn't send all of the string. I wonder why...\n";
				);
				Thread.delay 60.0;
				ping_loop ()
			) else (
				(* done *)
				print "DONE!\n"
			)
		) in
		ping_loop ()
	) in
	if o.o_adhoc_enabled && o.o_adhoc_agent <> 0 then (
		ignore (Thread.create ping_guts ())
	);

	(*******************)
	(* DISCOVER THREAD *)
	(*******************)
	let discover_guts initial_hash = (
		(* Start up processing the agents in the hash *)
		let print = (fun a -> print ~screen:false ~name:"DISCO1 " a) in

		Mutex.lock agent_hash_mutex;
		Array.iter (fun a ->
			let num_agents = fst a.ac_agents in
			let agents = Array.init num_agents (fun x -> {
				a1_current_job = None;
(*				a1_status = "Disconnected";*)
				a1_status = S_Disconnected;
				a1_total_frames = 0;
				a1_total_time = 0.0;
				a1_last_start = 0.0;
				a1_from_config = true;
				a1_thread = Thread.create disher_guts (a.ac_name, (a.ac_ip, a.ac_port), x);
			}) in
			Hashtbl.add agent_hash (a.ac_ip, a.ac_port) (a.ac_name, agents)
		) o.o_agents;
		Mutex.unlock agent_hash_mutex;

		(* Now keep checking the agents for anything new *)
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		(try
			Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, o.o_adhoc_controller));
		with
			x -> print "Dead with %S\n" (Printexc.to_string x)
		);
		let recv_string = String.create 640 in (* 640 should be enough for anybody *)
		let rec keep_going_unsafe = function
			| {range_next = None} -> false
			| {range_next = Some {range_type = Range_full _; range_right = next}} -> keep_going_unsafe next
			| {range_next = Some _ } -> true
		in
		(try
			while true do
				try
					let (got_bytes, from_addr) = Unix.recvfrom sock recv_string 0 (String.length recv_string) [] in
					Mutex.lock range_list_mutex;
					let keep_going = keep_going_unsafe range_list in
					Mutex.unlock range_list_mutex;
					if not keep_going then (
						print "all agents are done\n";
						raise End_of_file;
					) else if got_bytes >= 24 && String.sub recv_string 0 4 = "AGNT" && Digest.substring recv_string 0 (got_bytes - 16) = String.sub recv_string (got_bytes - 16) 16 then (
						(* Looks good *)
						let agent_ip = match from_addr with
							| Unix.ADDR_INET (ip,port) -> Unix.string_of_inet_addr ip
							| _ -> "0.0.0.0" (* Whut? *)
						in
						let agent_port = unpackn recv_string 4 in
						let agent_1st = unpackC recv_string 6 in
						let agent_2nd = unpackC recv_string 7 in
						let agent_name = if got_bytes > 24 then String.sub recv_string 8 (got_bytes - 24) else (Printf.sprintf "%s:%d" agent_ip agent_port) in

						print "got response from %s:%d = (%d,%d) agents under name %S\n" agent_ip agent_port agent_1st agent_2nd agent_name;

						Mutex.lock agent_hash_mutex;
						if agent_ip <> "0.0.0.0" && not (Hashtbl.mem agent_hash (agent_ip, agent_port)) then (
							let agents = Array.init agent_1st (fun x -> {
								a1_current_job = None;
(*								a1_status = "Disconnected";*)
								a1_status = S_Disconnected;
								a1_total_frames = 0;
								a1_total_time = 0.0;
								a1_last_start = 0.0;
								a1_from_config = false;
								a1_thread = Thread.create disher_guts (agent_name, (agent_ip, agent_port), x);
							}) in
							Hashtbl.add agent_hash (agent_ip, agent_port) (agent_name, agents)
						) else (
							print "agent already exists or IP address is bad (%s:%d)\n" agent_ip agent_port
						);
						Mutex.unlock agent_hash_mutex;
					) else (
						print "throwing out incorrect response %S\n" (String.sub recv_string 0 got_bytes);
					)
				with
				| End_of_file -> raise End_of_file
				| x -> (
					add_error "Agent discovery failed";
					print "failed with %S\n" (Printexc.to_string x);
					Thread.delay 10.0; (* A little time to cool down *)
				)
			done
		with
			End_of_file -> (
				print "exiting the discover thread\n";
				Unix.close sock;
			)
		)
	) in
	let discover_thread_perhaps = if o.o_adhoc_enabled && o.o_adhoc_controller <> 0 then (
		Some (Thread.create discover_guts ())
	) else (
		None
	) in
(*
	let dishing_threads = Array.mapi (fun i x -> Thread.create disher_guts (x,i)) o.o_agents_1 in
*)
	let splitter_thread = Thread.create splitter_guts () in
	Thread.join splitter_thread;
	print ~screen:false "Splitter kicked the bucket\n";

	(* Signal to the discover thread that the encode is done *)
	(match discover_thread_perhaps with
		| Some discover_thread -> (
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
			let sendthis = "KILL\x39\x86\xdd\x09\x14\x95\x8a\xe2\x11\x5e\x0a\xe7\xf1\x0b\xee\xf3" in
			ignore (Unix.sendto sock sendthis 0 (String.length sendthis) [] (Unix.ADDR_INET (Unix.inet_addr_loopback, o.o_adhoc_controller)));
			print ~screen:false "KILLING DISCOVER THREAD (hopefully)\n";
			Thread.join discover_thread;
			print ~screen:false "KILLED DISCOVER THREAD\n";
		)
		| None -> ()
	);

	print ~screen:false "Closing files and cleaning up directory\n";
	(* Output all the frames to the first_stats file *)

	(try
		let options_ok = Str.string_match options_rx !options_line_ref 0 in

		Mutex.lock range_list_mutex;
		let (final_array, final_array_ok) = match range_list with
			| {range_next = Some {range_type = Range_full frames; range_right = {range_next = None}}} -> (frames, Array.length frames = i.i_num_frames)
			| _ -> ([||], false)
		in

		if options_ok && final_array_ok then (
			let final_stats = open_out first_stats in
			Printf.fprintf final_stats "%s\n" !options_line_ref;
			let rec write_stats index = (
				if index < i.i_num_frames then (
					match final_array.(index) with
					| Frame_I x | Frame_notI x -> (
						Printf.fprintf final_stats "%s\n" x;
						write_stats (succ index)
					)
					| No_frame -> (
						(* This shouldn't happen *)
						add_error "First pass done but there are still unrendered frames; re-running first pass\n";
						close_out final_stats;
						close_out out_stats_handle;
						run_first_pass o i dir first_stats compression_setting supported_compressions
					)
				) else (
					close_out final_stats;
					close_out out_stats_handle;
					
					(* Remove EVERYTHING from the temp dir *)
					let dir_contents = Sys.readdir dir in
					Array.iter (fun x ->
						let full_name = Filename.concat dir x in
						(try
							if is_dir full_name then (
								Unix.rmdir full_name
							) else (
								Unix.unlink full_name
							)
						with _ -> ())
					) dir_contents;
					(try
						Unix.rmdir dir;
					with _ -> ())
				)
			) in
			write_stats 0
		) else (
			if final_array_ok then (
				add_error "Final options line not OK; re-rendering the first GOP to get it\n";
				output_string out_stats_handle "#DELETE 0 0\n";
			) else (
(*				print_ranges "WARNING: The first pass finished, but the resultant ranges are invalid. Re-running the first pass\n";*)
			);
			close_out out_stats_handle;
			run_first_pass o i dir first_stats compression_setting supported_compressions
		)
	with
		x -> add_error (Printf.sprintf "Output file probably not written (%S)\n" (Printexc.to_string x))
	)

;;
(* LE END *)
