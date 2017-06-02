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
open Controllerinclude;;
open Matroska;;
open Ratecontrol;;
open Pack;;
open Net;;




let obfuscate n = (
	let rec helper p q left = if left = 0 then q else (
		helper (p lsr 1) (q lsl 1 lor (p land 1)) (pred left)
	) in
	let bits = 31 in
	helper n (n lsr bits) bits
);;



let run_second_pass o i second_dir first_stats compression_setting supported_compressions =

	print "\n";
	print "SECOND PASS\n";

	(* Handy functions that I don't want to put in common/types.ml *)
	
	let version_rx = Str.regexp "#VERSION \\([0-9]+\\)" in
	(* #GOP number start_frame num_frames requested_bits actual_bits num_gops_done filename *)
	let gop_rx = Str.regexp "#GOP \\([0-9]+\\) \\([0-9]+\\) \\([0-9]+\\) \\([0-9.]+\\) \\([0-9.]+\\) \\([0-9]+\\) <\\([^>]*\\)>" in
	
	let options_rx = Str.regexp "#options: " in

	let gop_tree_mutex = Mutex.create () in
	let gop_tree_condition = Condition.create () in
(*	let gop_tree = Rbtree.create ~cmp:(fun (a1,a2) (b1,b2) -> if a1 <> b1 then compare a1 b1 else compare (a2 lxor 0x2AAAAAAA) (b2 lxor 0x2AAAAAAA)) () in*)
	let gop_tree = Rbtree.create ~cmp:(fun (a1,a2) (b1,b2) -> if a1 <> b1 then compare a1 b1 else compare (obfuscate a2) (obfuscate b2)) () in (* The custom cmp function is to avoid adding the GOPs in order, which is the worst-case for RB-trees and the standard way of x264farm to do things *)
(*	let gop_working_array = Array.make (Array.length o.o_agents_2) None in *) (* This holds any GOPs that are in use by an agent. This allows ratecontrol to work properly when stuff is still happening *)
	let expected_bits_so_far_ref = ref 0.0 in
	let requested_bits_so_far_ref = ref 0.0 in
	let actual_bits_so_far_ref = ref 0.0 in
	let dishers_working_ref = ref 0 in (* The number of dishers which have checked out a GOP. This is useful for checking if there is no need to continue due to all the GOPs being above the third pass threshold *)
	let second_pass_start_time = Sys.time () in
	let second_pass_frames_done_ref = ref 0 in
	let second_pass_frames_total_done_ref = ref 0 in (* This is the TOTAL number of frames that were done, including previous attempts *)

	let agent_hash = Hashtbl.create 10 in
	let agent_hash_mutex = Mutex.create () in
	let agent_hash_dead_condition = Condition.create () in


	(* Fill up the gop tree with goodies! *)
	let (out_stats_handle, in_name, out_name, options_line, gops_total, gops_already_done) = (
		let total_gops_ref = ref 0 in
		let pass1_stats = open_in first_stats in
		let temp_gop_tree = Rbtree.create () in (* Temp GOP array, indexed by GOP number *)
		let options_line_ref = ref "" in
		
		let rec add_to_gop gop_list_so_far current_gop_number current_gop_start_frame = (
			let new_line = (try
				input_line pass1_stats
			with
				End_of_file -> (
					(* I wish there was a better way to do this without throwing around a bunch of exceptions... *)
					let stats_line_array = Array.create (List2.length gop_list_so_far) null_stats_line in
					let stats_line_array_d = Array.create (List2.length gop_list_so_far) null_stats_line in
					List2.iter (fun x -> stats_line_array.(x.stats_in) <- x; stats_line_array_d.(x.stats_out) <- x) gop_list_so_far;

					let gop_end_frame = pred (List2.length gop_list_so_far) + current_gop_start_frame in
					let zone_string = List.fold_left (fun so_far -> function
						| {zone_end   = zb} when zb < current_gop_start_frame -> so_far (* Zones are outside range *)
						| {zone_start = za} when za > gop_end_frame -> so_far
						| {zone_start = za; zone_end = zb; zone_type = Zone_bitrate zf} -> (Printf.sprintf "%s%d,%d,b=%f" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - current_gop_start_frame)) (min (gop_end_frame - current_gop_start_frame) (zb - current_gop_start_frame)) zf)
						| {zone_start = za; zone_end = zb; zone_type = Zone_q zq} -> (Printf.sprintf "%s%d,%d,q=%d" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - current_gop_start_frame)) (min (gop_end_frame - current_gop_start_frame) (zb - current_gop_start_frame)) (int_of_float zq))
					) "" o.o_zones in

					let min_bits = Array.fold_left (fun so_far gnu -> so_far + gnu.stats_misc) first_frame_extra_bits stats_line_array in
					let min_bitrate = int_of_float (ceil (float_of_int min_bits /. (1000.0 *. float_of_int (Array.length stats_line_array) /. i.i_fps_f))) in

					let prev_gop = {
						gop_number = current_gop_number;
						gop_frame_offset = current_gop_start_frame;
						gop_optimal_bits = 0.0;
						gop_attempted_bits = None;
						gop_times_completed = 0;
						gop_frames = stats_line_array;
						gop_display_frames = stats_line_array_d;
						gop_zones = zone_string;
						gop_output_file_name = "";
						gop_initial_min_bitrate = min_bitrate;
						gop_current_min_bitrate = min_bitrate;
					} in
					Rbtree.add temp_gop_tree current_gop_number prev_gop;
					raise End_of_file
				)
			) in
			match stats_line_of_string new_line with
			| None -> (
				if Str.string_match options_rx new_line 0 then (
					(* It's options! *)
					print ~screen:false "got options\n";
					options_line_ref := new_line;
					add_to_gop gop_list_so_far current_gop_number current_gop_start_frame
				) else (
					print ~screen:false "Don't know what %S means\n" new_line);
					add_to_gop gop_list_so_far current_gop_number current_gop_start_frame
				)
			| Some ({stats_type = Stats_frame_I} as new_frame) -> (
				(* An I frame! *)
				incr total_gops_ref;
				(* Output the last GOP (if necessary) *)
				let final_zone = List.fold_left (fun so_far z -> if z.zone_start <= new_frame.stats_out && z.zone_end >= new_frame.stats_out then z.zone_type else so_far) (Zone_bitrate 1.0) o.o_zones in
				if List2.length gop_list_so_far > 0 then (
					(* Make the frame array (and the display frame array *)
					let stats_line_array = Array.create (List2.length gop_list_so_far) null_stats_line in
					let stats_line_array_d = Array.create (List2.length gop_list_so_far) null_stats_line in
					List2.iter (fun x -> stats_line_array.(x.stats_in) <- x; stats_line_array_d.(x.stats_out) <- x) gop_list_so_far;

					let gop_end_frame = pred (List2.length gop_list_so_far) + current_gop_start_frame in
					let zone_string = List.fold_left (fun so_far -> function
						| {zone_end   = zb} when zb < current_gop_start_frame -> so_far (* Zones are outside range *)
						| {zone_start = za} when za > gop_end_frame -> so_far
						| {zone_start = za; zone_end = zb; zone_type = Zone_bitrate zf} -> (Printf.sprintf "%s%d,%d,b=%f" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - current_gop_start_frame)) (min (gop_end_frame - current_gop_start_frame) (zb - current_gop_start_frame)) zf)
						| {zone_start = za; zone_end = zb; zone_type = Zone_q zq} -> (Printf.sprintf "%s%d,%d,q=%d" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - current_gop_start_frame)) (min (gop_end_frame - current_gop_start_frame) (zb - current_gop_start_frame)) (int_of_float zq))
					) "" o.o_zones in

					let min_bits = Array.fold_left (fun so_far gnu -> so_far + gnu.stats_misc) first_frame_extra_bits stats_line_array in
					let min_bitrate = int_of_float (ceil (float_of_int min_bits /. (1000.0 *. float_of_int (Array.length stats_line_array) /. i.i_fps_f))) in

					let prev_gop = {
						gop_number = current_gop_number;
						gop_frame_offset = current_gop_start_frame;
						gop_optimal_bits = 0.0;
						gop_attempted_bits = None;
						gop_times_completed = 0;
						gop_frames = stats_line_array;
						gop_display_frames = stats_line_array_d;
						gop_zones = zone_string;
						gop_output_file_name = "";
						gop_initial_min_bitrate = min_bitrate;
						gop_current_min_bitrate = min_bitrate;
					} in
					Rbtree.add temp_gop_tree current_gop_number prev_gop;


					(* Make a new GOP *)
					let new_list = List2.create () in
					let new_start_frame = new_frame.stats_in in
					new_frame.stats_in  <- 0;
					new_frame.stats_out <- 0;
					new_frame.stats_zone <- final_zone;
					List2.append new_list new_frame;
(*					print "New I frame %03d@%03d (%03d total) %S\n" (succ current_gop_number) new_start_frame (List2.length new_list) (string_of_stats_line new_frame);*)
					add_to_gop (new_list) (succ current_gop_number) new_start_frame
				) else (
					(* Don't bother with saving the GOP, cuz it doesn't exist *)
					let new_list = List2.create () in
					new_frame.stats_zone <- final_zone;
					List2.append new_list new_frame;
(*					print "1ST I frame %03d@%03d (%03d total) %S\n" current_gop_number current_gop_start_frame (List2.length new_list) (string_of_stats_line new_frame);*)
					add_to_gop new_list 0 new_frame.stats_in
				);
			)
			| Some new_frame -> (
				(* Just a normal frame *)
				let final_zone = List.fold_left (fun so_far z -> if z.zone_start <= new_frame.stats_out && z.zone_end >= new_frame.stats_out then z.zone_type else so_far) (Zone_bitrate 1.0) o.o_zones in
				new_frame.stats_in  <- new_frame.stats_in  - current_gop_start_frame;
				new_frame.stats_out <- new_frame.stats_out - current_gop_start_frame;
				new_frame.stats_zone <- final_zone;
				List2.append gop_list_so_far new_frame;
(*				print "other frame %03d@%03d (%03d total) %S\n" current_gop_number current_gop_start_frame (List2.length gop_list_so_far) (string_of_stats_line new_frame);*)
				add_to_gop gop_list_so_far current_gop_number current_gop_start_frame
			)
		) in
		(try
			add_to_gop (List2.create ()) 0 0
		with
			End_of_file -> close_in pass1_stats
		);


		(* PRINT EVERYTHANG! *)
(*
		Btree.print_tree temp_gop_tree (Printf.sprintf "%3d") (fun gop ->
			Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_zones
		);
*)
(*		let frametype_rx = Str.regexp "in:[0-9]+ out:\\([0-9]+\\) type:\\(.\\)" in*)
(*		let deleted_rx = Str.regexp "#DELETE \\([0-9]+\\)" in*)
		let version_rx = Str.regexp "#VERSION \\([0-9]+\\)" in
		(* Have 2 working stats files; one for reading and one for writing. This prevents any accidental screwing up when the program exits uncleanly *)
		(* The program will pick the largest file as the reading file *)
		let working_stats_name_1 = Filename.concat second_dir "working_stats1.txt" in
		let working_stats_name_2 = Filename.concat second_dir "working_stats2.txt" in

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

		(* Who's got the best stats file? *)
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
		
		(* Now read the input file! *)
		let working_gop_number_done_ref = ref 0 in (* This is updated to the total number of GOPs which have been processed, which is stored in each GOP line *)
		let largest_number_done_ref = ref 0 in (* The maximum of the previous numbers, updated every GOP end *)

		let parse_as_gop line = (
			if Str.string_match gop_rx line 0 then (
				let gop_number     = int_of_string   (Str.matched_group 1 line) in
				let gop_start      = int_of_string   (Str.matched_group 2 line) in
				let gop_num_frames = int_of_string   (Str.matched_group 3 line) in
				let gop_req_bits   = float_of_string (Str.matched_group 4 line) in
				let gop_real_bits  = float_of_string (Str.matched_group 5 line) in
				working_gop_number_done_ref := int_of_string (Str.matched_group 6 line);
				let gop_filename   =                 (Str.matched_group 7 line) in
				if is_file (Filename.concat second_dir gop_filename) then (
					Some {
						gop_number = gop_number;
						gop_frame_offset = gop_start;
						gop_optimal_bits = 0.0;
						gop_attempted_bits = Some (gop_req_bits, gop_real_bits);
						gop_times_completed = 1; (* It actually got done at least once, or it wouldn't be in the file *)
						gop_frames = Array.make gop_num_frames null_stats_line;
						gop_display_frames = Array.make gop_num_frames null_stats_line;
						gop_zones = "";
						gop_output_file_name = gop_filename;
						gop_initial_min_bitrate = 1;
						gop_current_min_bitrate = 1;
					}
				) else (
					None
				)
			) else (
				None
			)
		) in

		let print_log = (fun a -> Controllerinclude.print ~screen:false a) in
		let rec find_gop () = (
			let line = input_line in_stats_handle in
			print_log "Find GOP in \"%s\"\n" line;
			match parse_as_gop line with
			| Some gop -> (
				print_log " Found a GOP\n";
				find_frame gop
			)
			| None -> (
				(* Not found; keep going *)
				print_log " No GOP found (or file deleted)\n";
				find_gop ()
			)
		) and find_frame current_gop = (
			let line = input_line in_stats_handle in
(*			print "Finding stats in \"%s\"\n" line;*)
			match stats_line_of_string line with
			| Some y -> (
(*				print " Stats found\n";*)
				let final_zone = List.fold_left (fun so_far z -> if z.zone_start <= y.stats_out + current_gop.gop_frame_offset && z.zone_end >= y.stats_out + current_gop.gop_frame_offset then z.zone_type else so_far) (Zone_bitrate 1.0) o.o_zones in
				y.stats_zone <- final_zone;
				
				if y.stats_in < Array.length current_gop.gop_frames && y.stats_out < Array.length current_gop.gop_display_frames then (
					current_gop.gop_frames.(y.stats_in) <- y;
					current_gop.gop_display_frames.(y.stats_out) <- y;
				) else (
(*					print "  Frame is out of bounds! This GOP should be thrown out!\n";*)
				);
				
				find_frame current_gop
			)
			| None -> (
				(* Not a stats line *)
				match parse_as_gop line with
				| Some new_gop -> (
					(* Found another GOP beginning; throw out current GOP *)
(*					print " New GOP found; throw out the old one\n";*)
					find_frame new_gop
				)
				| None -> (
					(* Maybe it's a GOPEND *)
					if String.length line >= 7 && String.sub line 0 7 = "#GOPEND" then (
						(* Write the GOP *)
(*						print " GOPEND found\n";*)

						(* Let's make sure all the frames are there *)
						let gop_is_no_good = Array.fold_left (fun so_far next_stats -> so_far || next_stats == (* PHYSICAL EQUALITY *) null_stats_line) false current_gop.gop_frames in
						if gop_is_no_good then (
(*							print "  but it does not have all the frames!\n";*)
						) else (
							(* Update the minimum bitrate *)
							let min_bits = Array.fold_left (fun so_far gnu -> so_far + gnu.stats_misc) first_frame_extra_bits current_gop.gop_frames in
							let min_bitrate = int_of_float (ceil (float_of_int min_bits /. (1000.0 *. float_of_int (Array.length current_gop.gop_frames) /. i.i_fps_f))) in
							current_gop.gop_initial_min_bitrate <- min_bitrate;
							current_gop.gop_current_min_bitrate <- min_bitrate;
							
							Rbtree.add temp_gop_tree current_gop.gop_number current_gop;
						);


						find_gop ()
					) else (
						(* Something else; ignore! *)
(*						print " Something else; ignore\n";*)
						find_frame current_gop
					)
				)
			)
		) in


		(try
			find_gop ()
		with End_of_file -> ());




		close_in in_stats_handle;

		(* It's actually a bit difficult to find out if all the GOPs are actually done... *)
		let gops_with_files = (
			let rec find_gop now go_to so_far = (
(*				print "%d / %d (%d)\n" now go_to so_far;*)
				if now > go_to then so_far else (
					let gop_has_file = (match Rbtree.find temp_gop_tree now with
						| None -> false
						| Some (_,g) -> is_file (Filename.concat second_dir g.gop_output_file_name)
					) in
					if gop_has_file then (
						find_gop (succ now) go_to (succ so_far)
					) else (
						find_gop (succ now) go_to so_far
					)
				)
			) in
			find_gop 0 (pred !total_gops_ref) 0
		) in
		print ~screen:false "Number of GOPs done: %d / %d\n" gops_with_files !total_gops_ref;

		let gops_already_done = if gops_with_files = !total_gops_ref then (
			(* If all the GOPs have already been processed once, use the total number of GOPs which have been computed *)
			(* This is to prevent The third pass from restarting when the program restarts *)
			print ~screen:false "Looks like the second pass was already done; using %d for the number of GOPs computed\n" !largest_number_done_ref;
			!largest_number_done_ref
		) else (
			print ~screen:false "Looks like the second pass has not finished; using %d for the number of GOPs computed\n" gops_with_files;
			gops_with_files
		) in

		(* PRINT EVERYTHANG! *)
(*
		Btree.print_tree temp_gop_tree (Printf.sprintf "%3d") (fun gop ->
			Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_output_file_name
		);
*)
		(* Write the output file *)
		(* And put all the GOPs into the actual gop_tree *)
		(* And update the number of bits so far *)
		Rbtree.iter (fun _ gop -> match gop.gop_attempted_bits with
			| Some (req,got) -> (
				Printf.fprintf out_stats_handle "#GOP %d %d %d %f %f %d <%s>\n" gop.gop_number gop.gop_frame_offset (Array.length gop.gop_frames) req got gops_already_done gop.gop_output_file_name;
				Array.iter (fun x -> Printf.fprintf out_stats_handle "%s\n" (string_of_stats_line x)) gop.gop_frames;
				Printf.fprintf out_stats_handle "#GOPEND\n";
				Rbtree.add gop_tree (((min req got) /. (max req got)), gop.gop_number) gop;
				(* Just use the previous number of expected bits, as I don't think it will be any more accurate basing it on the ratecontrol value *)
				requested_bits_so_far_ref := !requested_bits_so_far_ref +. req;
				(* Uhh... this is wrong, but it should be right enough (and I only use expected bits for the precise file size calculation) *)
				expected_bits_so_far_ref := !expected_bits_so_far_ref +. req;
				actual_bits_so_far_ref := !actual_bits_so_far_ref +. got;
			)
			| _ -> (Rbtree.add gop_tree (0.0, gop.gop_number) gop) (* Don't output GOP if the second pass hasn't been done yet *)
		) temp_gop_tree;

(*
		Btree.print_tree gop_tree (fun (ratio,n) -> Printf.sprintf "%4d @ %f" n ratio) (fun gop ->
			Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_zones
		);
*)

		Printf.fprintf out_stats_handle "#VERSION %d\n" out_version;
		flush out_stats_handle;

		(out_stats_handle,in_name,out_name,!options_line_ref,!total_gops_ref,gops_already_done)
	) in


	(* Figure out how many frames were already done *)
	let frames_done_at_beginning = Rbtree.fold_left (fun so_far (ratio,num) gop -> if ratio = 0.0 then so_far else so_far + Array.length gop.gop_frames) 0 gop_tree in
	second_pass_frames_total_done_ref := frames_done_at_beginning;

	(* These settings were called the third pass settings on the previous version. Now it's global *)
	(* gops_done_ref is now protected by gop_tree_mutex, since that's where all the other updating is done *)
	let pass_threshold = min o.o_third_pass_threshold (1.0 /. o.o_third_pass_threshold) in
	let max_gops_to_do = gops_total + (min o.o_third_pass_max_gops (int_of_float (float_of_int gops_total *. o.o_third_pass_max_gop_ratio))) in
	let gops_done_ref = ref gops_already_done in
	print ~screen:false "Max GOP threshold is %d GOPs (out of %d) or a ratio of %f\n" max_gops_to_do gops_total pass_threshold;





	(*************)
	(* NEW PRINT *)
	(*************)
	let p_term_width = if use_console_functions then (
		let info = Console.get_console_screen_buffer_info Console.std_output_handle in
		info.Console.dwSizeX
	) else 80 in

	let p_error_array_mutex = Mutex.create () in
	let p_error_length = 4 in
	let p_error_oldest_ref = ref 0 in (* The index of the oldest error in the array *)
	let p_error_array = Array.make p_error_length ("~","") in (* The error array is actually cyclical *)
	let p_error_array_temp = Array.make p_error_length ("","") in (* This is only used for copying from error_array, to avoid needing two locks at the same time *)

	let p_lines_overhead = p_error_length + 5 in
	let p_current_lines_ref = ref 0 in

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

		Mutex.lock gop_tree_mutex;
		let frames_done = Rbtree.fold_left (fun so_far (ratio,_) gop -> if ratio > 0.0 then so_far + Array.length gop.gop_frames else so_far) 0 gop_tree in
		Mutex.unlock gop_tree_mutex;
		let fps = (float_of_int (frames_done - frames_done_at_beginning) /. (Sys.time () -. second_pass_start_time)) in
		let fps_line = Printf.sprintf "%d%% done (%d / %d) at %.2f FPS" (int_of_float (100.0 *. (float_of_int frames_done /. float_of_int i.i_num_frames))) frames_done i.i_num_frames fps in

		(* Time taken *)
		let time_so_far_line = Printf.sprintf "Last updated: %s" (time_of_seconds (Sys.time ())) in

		(* ETA *)
		let eta = float_of_int (i.i_num_frames - frames_done) /. fps in
		let eta_line = "ETA: " ^ (time_of_seconds eta) in

		(* Disher status *)
		Mutex.lock agent_hash_mutex;
		let (name_list, fps_list, status_list_list) = Hashtbl.fold (fun (ip,port) (name,agent_array) (n,f,s) ->
			let (fps, status_list) = Array.fold_left (fun (fps_so_far,status_so_far) gnu ->
				((if gnu.a2_total_time = 0.0 then fps_so_far else fps_so_far +. float_of_int gnu.a2_total_frames /. gnu.a2_total_time), gnu.a2_status :: status_so_far)
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
					| "" -> n.a2_status
					| _ -> status_so_far ^ " ~ " ^ n.a2_status
				in
				(fps_so_far +. float_of_int n.a2_total_frames /. n.a2_total_time, new_status)
			) (0.0, "") agent_array in
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
		print ~lock:false ~fill:true " %s" eta_line;
		print ~lock:false ~fill:true " %*s | %-*s | Description" max_name "Agent" max_fps "FPS";
		List.iter (fun (name, fps, status) ->
			print ~lock:false ~fill:true " %*s | %-*s | %s" max_name name max_fps fps (if String.length status > max_status then String.sub status 0 (max_status - 3) ^ "..." else status)
		) string_list;
		print ~lock:false ~fill:true "Recent errors:";
		Array.iter (fun (t,e) -> let x = Printf.sprintf "%*s %s" max_error_time_length t e in print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 5) ^ "..." else x)) p_error_array_temp;
		Mutex.unlock pm;
	) in

	let update_status disher_key disher_index status = (
		Mutex.lock agent_hash_mutex;
		let old_status = (snd (Hashtbl.find agent_hash disher_key)).(disher_index).a2_status in
		Mutex.unlock agent_hash_mutex;
		if status <> old_status then (
			(* Lock it again! (there's probably a better way to do this...) *)
			Mutex.lock agent_hash_mutex;
			(snd (Hashtbl.find agent_hash disher_key)).(disher_index).a2_status <- status;
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
		(snd (Hashtbl.find agent_hash disher_key)).(disher_index).a2_last_start <- Sys.time ();
		Mutex.unlock agent_hash_mutex;
	) in
	let fps_disher_stop disher_key disher_index frames = (
		Mutex.lock agent_hash_mutex;
		let a = (snd (Hashtbl.find agent_hash disher_key)).(disher_index) in
		a.a2_total_frames <- a.a2_total_frames + frames;
		a.a2_total_time <- a.a2_total_time +. Sys.time () -. a.a2_last_start;
		Mutex.unlock agent_hash_mutex;
	) in

	let change_working_job disher_key disher_index job = (
		Mutex.lock agent_hash_mutex;
		(snd (Hashtbl.find agent_hash disher_key)).(disher_index).a2_current_job <- job;
		Mutex.unlock agent_hash_mutex;
	) in

	let print_agent_hash_unsafe () = (
		print ~screen:false ~lock:false "AGENT HASH:\n";
		Hashtbl.iter (fun (ip,port) (name,agent_array) ->
			print ~screen:false ~lock:false "  %s:%05d %S:\n" ip port name;
			Array.iteri (fun i x ->
				print ~screen:false ~lock:false "    %d:\n" i;
				print ~screen:false ~lock:false "      Current job:  %s\n" (match x.a2_current_job with | None -> "NONE" | Some gop -> Printf.sprintf "GOP number %d" gop.gop_number);
				print ~screen:false ~lock:false "      Status:       %s\n" "???" (*x.a2_status*);
				print ~screen:false ~lock:false "      Total frames: %d\n" x.a2_total_frames;
				print ~screen:false ~lock:false "      Total time:   %.2f\n" x.a2_total_time;
				print ~screen:false ~lock:false "      Last start:   %.2f\n" x.a2_last_start;
				print ~screen:false ~lock:false "      From config?  %B\n" x.a2_from_config;
				print ~screen:false ~lock:false "      Kill OK?      %B\n" x.a2_kill_ok;
			) agent_array
		) agent_hash;
	) in

	let print_agent_hash_pm () = (
		Mutex.lock pm;
		print_agent_hash_unsafe ();
		Mutex.unlock pm;
	) in

	let print_agent_hash = (
		let rec lock2 a b = ( (* Efficiently locks two mutexes at the same time, while not creating deadlocks *)
			Mutex.lock a;
			if Mutex.try_lock b then (
				()
			) else (
				Mutex.unlock a;
				lock2 b a
			)
		) in
		fun () -> (
			lock2 pm agent_hash_mutex;
			print_agent_hash_unsafe ();
			Mutex.unlock pm;
			Mutex.unlock agent_hash_mutex;
		)
	) in

	(****************)
	(* STATIC PRINT *)
	(****************)
(*
	let p_disher_names = Array.map (fun (name,(ip,pf,pt),n) -> name ^ " " ^ string_of_int n) o.o_agents_2 in
	let p_disher_status_mutex = Mutex.create () in
	let p_disher_status = Array.map (fun _ -> "Disconnected") o.o_agents_2 in

	let p_term_width = if use_console_functions then (
		let info = Console.get_console_screen_buffer_info Console.std_output_handle in
		info.Console.dwSizeX
	) else 80 in

	let p_error_array_mutex = Mutex.create () in
	let p_error_length = 4 in
	let p_error_oldest_ref = ref 0 in (* The index of the oldest error in the array *)
	let p_error_array = Array.make p_error_length ("~","") in (* The error array is actually cyclical *)
	let p_error_array_temp = Array.make p_error_length ("","") in (* This is only used for copying from error_array, to avoid needing two locks at the same time *)

	(* Variables handling per-agent FPS counts *)
	let p_fps_array_mutex = Mutex.create () in
	let p_fps_frames = Array.map (fun _ -> 0) o.o_agents_2 in
	let p_fps_time = Array.map (fun _ -> 0.0) o.o_agents_2 in
	let p_fps_last_disher_start = Array.map (fun _ -> 0.0) o.o_agents_2 in

	(* Add the proper amount of padding in the output *)
	let p_lines = Array.length o.o_agents_2 + p_error_length + 5 in
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
		Mutex.lock gop_tree_mutex;
		let frames_done = Rbtree.fold_left (fun so_far (ratio,_) gop -> if ratio > 0.0 then so_far + Array.length gop.gop_frames else so_far) 0 gop_tree in
		Mutex.unlock gop_tree_mutex;
		let fps = (float_of_int (frames_done - frames_done_at_beginning) /. (Sys.time () -. second_pass_start_time)) in
		let fps_line = Printf.sprintf "%d%% done (%d / %d) at %.2f FPS" (int_of_float (100.0 *. (float_of_int frames_done /. float_of_int i.i_num_frames))) frames_done i.i_num_frames fps in

		(* Time taken *)
		let time_so_far_line = Printf.sprintf "Last updated: %s" (time_of_seconds (Sys.time ())) in

		(* ETA *)
		let eta = float_of_int (i.i_num_frames - frames_done) /. fps in
		let eta_line = "ETA: " ^ (time_of_seconds eta) in

		(* Disher status *)
		(* Start with length 5 for the string "Agent" *)
		let max_disher_length = Array.fold_left (fun so_far gnu -> max (String.length gnu) so_far) 5 p_disher_names in

		Mutex.lock p_fps_array_mutex;
		let fps_string_array = Array.mapi (fun i time -> if time <= 0.0 then "?" else Printf.sprintf "%.2f" (float_of_int p_fps_frames.(i) /. time)) p_fps_time in
		Mutex.unlock p_fps_array_mutex;
		let max_fps_length = Array.fold_left (fun so_far gnu -> max so_far (String.length gnu)) 3 fps_string_array in

		Mutex.lock p_disher_status_mutex;
		let disher_strings = Array.init (Array.length o.o_agents_2) (fun x ->
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
(*		print ~lock:false "\n\n\n\n";*)
		print ~lock:false ~fill:true " %s" fps_line;
		print ~lock:false ~fill:true " %s" time_so_far_line;
		print ~lock:false ~fill:true " %s" eta_line;
		print ~lock:false ~fill:true " %*s | %-*s | Description" max_disher_length "Agent" max_fps_length "FPS";
		Array.iter (fun x -> print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) disher_strings;
		print ~lock:false ~fill:true "Recent errors:";
(*		Array.iter (fun x -> print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) p_error_array_temp;*)

		Array.iter (fun (t,e) -> let x = Printf.sprintf "%*s %s" max_time_length t e in print ~lock:false ~fill:true " %s" (if String.length x > p_term_width - 2 then String.sub x 0 (p_term_width - 2) else x)) p_error_array_temp;

		Mutex.unlock pm;

	) in

	let print_status () = (
		print_everything ()
	) in
	
	let update_status disher status = (
		if status <> p_disher_status.(disher) then (
			Mutex.lock p_disher_status_mutex;
			p_disher_status.(disher) <- status;
			Mutex.unlock p_disher_status_mutex;
			print_status ()
		)
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
*)


	(***************)
	(* RATECONTROL *)
	(***************)
	print ~screen:false "YOU GOT TO THE RATECONTROL! %f bits / %f bits, or mult %f\n" !requested_bits_so_far_ref !actual_bits_so_far_ref (!requested_bits_so_far_ref /. !actual_bits_so_far_ref);
	let all_available_bits_float = (match o.o_second_pass_bitrate with
		| Percent i -> (
			let (fp_var_bits, fp_const_bits) = Rbtree.fold_left (fun (vb, cb) _ gop ->
				let (vb_new, cb_new) = Array.fold_left (fun (vb, cb) f ->
					(
						vb +. float_of_int (f.stats_itex + f.stats_ptex + f.stats_mv),
						cb +. float_of_int f.stats_misc
					)
				) (0.0,0.0) gop.gop_frames in
				(vb +. vb_new, cb +. cb_new)
			) (0.0,0.0) gop_tree in
			(fp_var_bits +. fp_const_bits) *. float_of_int i *. 0.01
		)
		| Kbps f -> (
			1000.0 *. f *. float_of_int i.i_num_frames /. i.i_fps_f
		)
	) in
	print ~screen:false "%f bits for usage\n" all_available_bits_float;
	let rc_gop_tree gop_tree = (
		let num_macroblocks = ((i.i_res_x + 15) / 16) * ((i.i_res_y + 15) / 16) in
(*		let rate_factor = rate_control_gop_tree gop_tree num_macroblocks 20 all_available_bits_float 100.0 0.001 in*)
		ignore (rate_control_gop_tree gop_tree num_macroblocks o all_available_bits_float 100.0 0.001);
		Rbtree.iter (fun _ gop ->
			gop.gop_optimal_bits <- (
				Array.fold_left (fun so_far f ->
					so_far +. bits_of_qscale f f.stats_out_blurqscale
				) 0.0 gop.gop_frames
			)
		) gop_tree
	) in
	rc_gop_tree gop_tree;
(*
	Rbtree.iter (fun (div,num) gop -> 
		let min_bits = Array.fold_left (fun so_far f -> so_far + f.stats_misc) 0 gop.gop_frames in
		print "GOP %4d (%3d frames) @ %f = %11.2f bits, or %6f kbps, min %6d (%f kbps)\n" num (Array.length gop.gop_frames) div gop.gop_optimal_bits (gop.gop_optimal_bits /. (1000.0 *. float_of_int (Array.length gop.gop_frames) /. i.i_fps_f)) min_bits (float_of_int min_bits /. (1000.0 *. float_of_int (Array.length gop.gop_frames) /. i.i_fps_f))
	) gop_tree;
*)
	Rbtree.kpretty_print (fun x -> print ~screen:false "%s" x) gop_tree (fun (div,num) -> Printf.sprintf "%4d @ %6f" num div) (fun gop -> Printf.sprintf "%3d frames, %11.2f bits (%8.2f kbps), min %6d (%6.2f kbps)" (Array.length gop.gop_frames) gop.gop_optimal_bits (gop.gop_optimal_bits /. (1000.0 *. float_of_int (Array.length gop.gop_frames) /. i.i_fps_f)) (Array.fold_left (fun so_far f -> so_far + f.stats_misc) 0 gop.gop_frames) (float_of_int (Array.fold_left (fun so_far f -> so_far + f.stats_misc) 0 gop.gop_frames) /. (1000.0 *. float_of_int (Array.length gop.gop_frames) /. i.i_fps_f)));

	
(*	exit 1;*)
(*
	Btree.print_tree gop_tree (fun (div,gop) -> Printf.sprintf "%d = %f" gop div) (fun gop ->
		(Printf.sprintf "#%03d@%06d %fbits %S\n" gop.gop_number gop.gop_frame_offset gop.gop_optimal_bits gop.gop_zones)
		^
		(Array.fold_left (fun so_far stats ->
			so_far ^ (Printf.sprintf "  %s %S\n" (string_of_stats_line stats) (match stats.stats_zone with | Zone_bitrate x -> "B=" ^ string_of_float x | Zone_q x -> "Q=" ^ string_of_float x))
		) "" gop.gop_frames)
	);
*)

	(******************)
	(* Check for more *)
	(******************)
	(* This is now no longer part of every thread *)
	(* Must be protected by gop_tree_mutex *)
	let check_for_more_jobs_unsafe () = (
		if !gops_done_ref > max_gops_to_do then false else (
			match Rbtree.first gop_tree with
			| Some ((first_ratio,_),_) when first_ratio <= pass_threshold -> true
			| _ -> false
		)
	) in
	let check_for_more_jobs () = (
		Mutex.lock gop_tree_mutex;
		let a = check_for_more_jobs_unsafe () in
		Mutex.unlock gop_tree_mutex;
		a
	) in

	(***********)
	(* DISHERS *)
	(***********)
	let disher_guts (name,(ip,port),n) = (
		let name_num = name ^ " " ^ string_of_int n in
		let print = (fun a -> print ~screen:false ~name:(name_num ^ " ") a) in
		let print_log = (fun a -> Controllerinclude.print ~screen:false ~name:(name ^ " " ^ string_of_int n ^ " ") a) in
		print "starting on %s (id %d)\n" ip (Thread.id (Thread.self ()));
		let update = update_status (ip,port) n in
		let fps_start () = fps_disher_start (ip,port) n in
		let fps_stop = fps_disher_stop (ip,port) n in
		let change_working_job = change_working_job (ip,port) n in
(*
		let check_for_more_jobs_unsafe () = (
			let rec try_checkout () = (
				match Rbtree.first gop_tree with
				| Some ((first_ratio,_),_) -> (
					if first_ratio > pass_threshold then (
						(* Nothing to do now *)
						if !dishers_working_ref = 0 then (
							(* Nobody else is working; that means we're done! *)
							false
						) else (
							(* Somebody's working; wait till they're done *)
(*							update "Waiting for job";*)
							update S_Waiting;
							Condition.wait gop_tree_condition gop_tree_mutex;
							try_checkout ()
						)
					) else (
						(* There is a GOP less than the threshold; try to connect *)
						true
					)
				)
				| None -> false (* Uhh... No GOPs? *)
			) in
			if !gops_done_ref > max_gops_to_do then false else try_checkout ()
		) in
		let check_for_more_jobs () = (
			Mutex.lock gop_tree_mutex;
			let a = check_for_more_jobs_unsafe () in
			Mutex.unlock gop_tree_mutex;
			a
		) in
*)
(*
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in

		let send = send sock in
		let recv () = recv sock in
*)

(* THREAD STRING *)
		let thread_string = String.create (max_string_length + 8) in
(*		let max_string_length_bin = packN max_string_length in*)
		String.blit "FRAM" 0 thread_string 0 4;
		String.blit max_string_length_bin 0 thread_string 4 4;

		let timeout = 5 in

		(* THIS IS THE BIG STATE MACHINE *)
		(* It should go:
			attempt_connection -> (check_out_job -> process_job -> return_job) -> close_connection
			return_job calls check_out_job as many times as there is stuff to do
			close_connection calls attempt_connection
		*)
		let rec attempt_connection () = (
			let sock = Unix.socket Unix.PF_INET Unix.SOCK_STREAM 0 in
			let send = send sock in
			let recv () = recv sock in

			if not (check_for_more_jobs ()) then (
				print_log "attempt_connect sees no jobs left; DEAD\n";
				()
			) else (
				try (
					Unix.connect sock (Unix.ADDR_INET (Unix.inet_addr_of_string ip, port));
					send "HELO" "";
(*					update ("Connected to " ^ ip ^ ":" ^ string_of_int port);*)
					update (S_Connected (ip,port));

					send "SCOM" supported_compressions;

					(* Ignore the first pass compression settings *)
					(match recv () with
						| ("SCO1", x) -> ()
						| x -> raise (unexpected_packet x)
					);

					let compress = (match recv () with
						| ("SCO2", x) -> (
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
				| _ -> (
					Unix.close sock;
(*					update "Disconnected";*)
					update S_Disconnected;
					Unix.sleep timeout;
					attempt_connection ()
				)
(*
				| Unix.Unix_error (x,y,z) -> (print "attempt_connection failed with (%S,%s,%s); trying again\n" (Unix.error_message x) y z; attempt_connection (succ port))
				| x -> (print "attempt_connection failed with %S; trying again\n" (Printexc.to_string x); attempt_connection (succ port))
*)
			)


		) and check_out_job sock send recv port compress = (
			Mutex.lock gop_tree_mutex;
			let a_gop_perhaps = if check_for_more_jobs_unsafe () then (
				match Rbtree.take_first gop_tree with
				| None -> None
				| Some ((ratio,_),gop) -> (
(*					gop_working_array.(index) <- Some gop;*)
					change_working_job (Some gop); (* FINE. Lock two mutexes at once. See if I care. *)
					
					incr dishers_working_ref;
					Some (ratio,gop,gop.gop_optimal_bits) (* The optimal_bits is here because the RC thread may change it while doing something. We don't want the new optimal bits to be used, as the bitrate was based on the old optimal bits *)
				)
			) else (
				None
			) in

(*			Rbtree.print_ordered gop_tree (fun (ratio,num) -> Printf.sprintf "%4d @ %f" num ratio) (fun gop -> Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_zones);*)

			Mutex.unlock gop_tree_mutex;

			match a_gop_perhaps with
			| None -> (
				print_log "check_out_job sees no jobs left; disconnect\n";
				close_connection sock send recv port
			)
			| Some (ratio,gop,optimal_bits) -> (
				print_log "check_out_job got GOP #%d from %.2f, with %.1f optimal bits\n" gop.gop_number ratio optimal_bits;

				process_job sock send recv port compress gop ratio optimal_bits
			)


		) and process_job sock send recv port compress gop orig_ratio optimal_bits = (
				
			let a = gop.gop_frame_offset in
			let b = a + Array.length gop.gop_frames - 1 in
			
			let first_frame = max 0 (a - o.o_preseek) in

			try (
				let zone_string = List.fold_left (fun so_far -> function
					| {zone_end   = zb} when zb < a -> so_far (* Zones are outside range *)
					| {zone_start = za} when za > b -> so_far
					| {zone_start = za; zone_end = zb; zone_type = Zone_bitrate zf} -> (Printf.sprintf "%s%d,%d,b=%f" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - a)) (min (b - a) (zb - a)) zf)
					| {zone_start = za; zone_end = zb; zone_type = Zone_q zq} -> (Printf.sprintf "%s%d,%d,q=%d" (if so_far = "" then "" else so_far ^ "/") (max 0 (za - a)) (min (b - a) (zb - a)) (int_of_float zq))
				) "" o.o_zones in
				
				let favs_md5 = Digest.file o.o_input_avs_full in
				let favs_send = List.fold_left (fun so_far gnu -> if so_far = "" then gnu else so_far ^ "/" ^ gnu) "" (split_on_dirs o.o_input_avs_full) in
				send "FAVS" (favs_md5 ^ favs_send);
				print_log "sent full AVS info %s = \"%s\"\n" (to_hex favs_md5) favs_send;
				
				send "VNFO" ((packN i.i_res_x) ^ (packN i.i_res_y) ^ (packN i.i_fps_n) ^ (packN i.i_fps_d));
				print_log "sent video info %dx%d @ %d/%d\n" i.i_res_x i.i_res_y i.i_fps_n i.i_fps_d;
				
				send "ZONE" zone_string;
				print_log "sent zone string %S\n" zone_string;

				send "RANG" ((packN (a + o.o_seek)) ^ (packN (b + o.o_seek)));
				print_log "sent range (%d,%d)\n" (a + o.o_seek) (b + o.o_seek);
				
				send "2PAS" (Printf.sprintf "%s --qcomp %F --cplxblur %F --qblur %F --qpmin %d --qpmax %d --qpstep %d --ipratio %F --pbratio %F" o.o_second o.o_rc_qcomp o.o_rc_cplxblur o.o_rc_qblur o.o_rc_qpmin o.o_rc_qpmax o.o_rc_qpstep o.o_rc_ipratio o.o_rc_pbratio);
				
				let add_some_bits = (if gop.gop_number <> 0 then first_frame_extra_bits else 0) in
				let add_some_bits_float = float_of_int add_some_bits in
				(* TARGET_BITS INCLUDES SEI BITS *)
				let target_bits = (match gop.gop_attempted_bits with
					| None -> (
						print_log "no attempted bits found; using %f / %f (+%d)\n" !requested_bits_so_far_ref !actual_bits_so_far_ref add_some_bits;
						if !actual_bits_so_far_ref = 0.0 then optimal_bits +. add_some_bits_float else optimal_bits *. !requested_bits_so_far_ref /. !actual_bits_so_far_ref +. add_some_bits_float
					)
					| Some (want,got) -> (
						print_log "found attempted bits %f -> %f\n" want got;
						if want <= 1.0 then (
(*
							print "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ WHAT IS GOING ON?!\n";
							print "actual_bits_so_far_ref:    %f\n" !actual_bits_so_far_ref;
							print "optimal_bits:              %f\n" optimal_bits;
							print "add_some_bits_float:       %f\n" add_some_bits_float;
							print "requested_bits_so_far_ref: %f\n" !requested_bits_so_far_ref;
*)
							if !actual_bits_so_far_ref = 0.0 then optimal_bits +. add_some_bits_float else optimal_bits *. !requested_bits_so_far_ref /. !actual_bits_so_far_ref +. add_some_bits_float
						) else (
							optimal_bits *. want /. got +. add_some_bits_float
						)
					)
				) in

				let precise_size_bits = (
					if !actual_bits_so_far_ref = 0.0 || (all_available_bits_float -. !expected_bits_so_far_ref) <= 0.0 then (
						print_log "precise bits not precise enough... using %f\n" target_bits;
						target_bits
					) else (
						let a = (all_available_bits_float -. !actual_bits_so_far_ref) /. (all_available_bits_float -. !expected_bits_so_far_ref) *. !requested_bits_so_far_ref /. !actual_bits_so_far_ref in
						print_log "using %f for precise file size multiplier\n" a;
						a *. optimal_bits +. add_some_bits_float
					)
				) in


				let target_bitrate = (
					let min_kbps = gop.gop_current_min_bitrate in

(*print "%f / (%d) * %f / 1000 + 0.5\n" target_bits (Array.length gop.gop_frames) i.i_fps_f;*)

					let target_or_precise_bits = (
						let fade_frames = max 500.0 (float_of_int i.i_num_frames *. 0.02) in
						let fade_now = o.o_size_precision *. min (min (float_of_int !second_pass_frames_total_done_ref /. fade_frames) ((float_of_int (i.i_num_frames - !second_pass_frames_total_done_ref)) /. fade_frames)) 1.0 in
						print_log "fade is now %f from %f to %f bits\n" fade_now target_bits precise_size_bits;
						precise_size_bits *. fade_now +. target_bits *. (1.0 -. fade_now)
					) in

					let optimal_kbps = int_of_float ((target_or_precise_bits /. (float_of_int (Array.length gop.gop_frames)) *. i.i_fps_f /. 1000.0) +. 0.5) in

					print_log "optimal bitrate is %d; minimum is %d\n" optimal_kbps min_kbps;
					if min_kbps > optimal_kbps then (
						min_kbps
					) else (
						optimal_kbps
					)
				) in
				send "TBIT" (Printf.sprintf "%d" target_bitrate);
				print_log "sent target bitrate %d (really want %f)\n" target_bitrate (optimal_bits /. (float_of_int (Array.length gop.gop_frames)) *. i.i_fps_f /. 1000.0);
				
				send "STAT" options_line;
				Array.iteri (fun i f ->
					if i = 0 then (
						(* Add some bits to the first frame to account for the SEI added to each encode *)
						send "STAT" (string_of_stats_line ~add_bits:add_some_bits f)
					) else (
						send "STAT" (string_of_stats_line f)
					)
				) gop.gop_frames;
				send "EOF!" "";

				let recvd = recv () in
(*
				if recvd = ("ENCD","agent") then (
					print_ref "agent-based encoding; waiting for agent to finish\n";
				) else if recvd = ("ENCD","controller") then (
(*					print "controller-based encoding; sending data\n";*)
				);
*)

				
				(try
					fps_start ();
					(match recvd with
						| ("ENCD","agent") -> (
(*							update (Printf.sprintf "Doing frames %d-%d (%d)" a b (b - a + 1));*)
							update (S_Doing (a,b));
        	
							let rec grab_output () = (
								(match Unix.select [sock] [] [] (30.0) with
									| ([x],[],[]) -> (
										(match recv () with
											| ("INDY","") -> (
												grab_output ()
											)
											| ("DONE","") -> () (* Done! *)
											| x -> raise (unexpected_packet x)
										)
									)
									| _ -> (
(*										update "Dead?";*)
										update S_Dead;
										raise Timeout
									)
								);
							) in
							grab_output ()
						)
						| ("ENCD","controller") -> (
        	
(*							update (Printf.sprintf "Sending frames %d-%d (%d)" a b (b - a + 1));*)
							update (S_Sending (a,b));
        	
(*							let total_bytes_64 = Int64.mul (Int64.of_int (Array.length gop.gop_frames)) (Int64.of_int i.i_bytes_per_frame) in*)
							let get = Unix.open_process_in (Printf.sprintf "\"\"%s\" -raw -seek %d -frames %d -o - \"%s\" 2>%s\"" i.i_avs2yuv (first_frame + o.o_seek) (b - first_frame + 1) o.o_input_avs dev_null) in
							if first_frame <> a then (
								let ignore_bytes = (a - first_frame) * i.i_bytes_per_frame in
								let ignore_string = String.create ignore_bytes in
								really_input get ignore_string 0 ignore_bytes
							);
        	
							(* COMPRESSION! *)
							(try
								compress (Array.length gop.gop_frames) get sock;
								ignore (Unix.close_process_in get);
							with
								x -> (
									ignore (Unix.close_process_in get);
									raise x
								)
							);
							
							send "EOF!" "";
						)
						| x -> (
							raise (unexpected_packet x)
						)
					);
					fps_stop (b - a + 1);
				with
					| Timeout -> (
(*						add_error "Timeout has been thrown. Let's see how well you... handle it";*)
						raise Timeout; (* On second thought... make something else handle it *)
					)
					| x -> (
						if target_bitrate <= 3 * gop.gop_initial_min_bitrate && gop.gop_current_min_bitrate < 3 * gop.gop_initial_min_bitrate then (
							(* The bitrate is pretty low. Let's see if raising the minimum will help anything *)
							
							let new_min = (11 * gop.gop_current_min_bitrate - 1) / 10 + 1 in
							print_log "looks like the encoding failed. Raising the minimum bitrate from %d to %d (real min %d)\n" gop.gop_current_min_bitrate new_min gop.gop_initial_min_bitrate;
							add_error (Printf.sprintf "%s encoding failed at %dkbps. Raising bitrate" name_num new_min);
							gop.gop_current_min_bitrate <- new_min;
						);
					)
				);
				
				(* Receive the stats *)
				let rec recv_stats () = (
					match recv () with
					| ("STAT",x) -> (
						match stats_line_of_string x with
						| Some s -> (
							(* STATS LINE! *)
							if s.stats_in = 0 then (
								(* Subtract the SEI bits before storage *)
								s.stats_misc <- max 0 (s.stats_misc - add_some_bits);
							);
							gop.gop_frames.(s.stats_in) <- s;
							gop.gop_display_frames.(s.stats_out) <- s;
							recv_stats ()
						)
						| None -> (
							if Str.string_match options_rx x 0 then (
								(* Option line; throw out *)
								recv_stats ()
							) else (
								add_error (Printf.sprintf "WARNING: Don't know how to handle %S\n" x);
								recv_stats ()
							)
						)
					)
					| ("EOF!","") -> (print_log "done receiving stats\n")
					| x -> raise (unexpected_packet x)
				) in

					recv_stats ();
				print_log "got stats\n";
(*				update (Printf.sprintf "Done with frames %d-%d" a b);*)
				update (S_Done (a,b));

(*
				let output_file_name = Printf.sprintf "%d - %d %d.mkv" gop.gop_number a b in
				let mkv_handle = open_out_bin (Filename.concat second_dir output_file_name) in
*)
				let (abs_output_file_name, mkv_handle) = open_temp_file ~mode:[Open_binary] (Filename.concat second_dir (Printf.sprintf "%d - %d %d " gop.gop_number a b)) ".mkv" in
				let output_file_name = Filename.basename abs_output_file_name in (* This makes the file relative to second_dir *)
				(try
					let rec recv_file () = (
						match recv () with
						| ("MOOV",x) -> (output_string mkv_handle x; recv_file ())
						| ("EOF!","") -> (print_log "done receiving mkv\n")
						| x -> raise (unexpected_packet x)
					) in
					recv_file ();
					close_out mkv_handle;
				with
					x -> (close_out mkv_handle; raise x)
				);
(*					print "got MKV (put in \"%s\")\n" (Filename.concat second_dir output_file_name);*)

				(* INFO *)
(*
				Mutex.lock info_mutex;
				info.i2_total_gops <- succ info.i2_total_gops;
				info.i2_total_frames <- info.i2_total_frames + Array.length gop.gop_frames;
				info.i2_current_gop <- None;
				Mutex.unlock info_mutex;
*)
				(* !INFO! *)

				return_job sock send recv port compress orig_ratio gop target_bits optimal_bits target_bits output_file_name
				
			) with
			| x -> (
				(match x with
					| Unix.Unix_error (x,y,z) -> print_log "process_job failed with (%S,%s,%s); putting job back\n" (Unix.error_message x) y z
					| x -> print_log "process_job failed with %S; putting job back\n" (Printexc.to_string x)
				);

(*				update (Printf.sprintf "Frames %d-%d failed" a b);*)
				update (S_Failed (a,b));
				(match x with
					| Unix.Unix_error (x,y,z) -> add_error (Printf.sprintf "%s failed with error %s on %s" name_num (unix_error_string x) y)
					| Timeout -> add_error (Printf.sprintf "%s timed out" name_num)
					| x -> add_error (Printf.sprintf "%s failed with \"%s\"" name_num (Printexc.to_string x))
				);
(*
				Mutex.lock info_mutex;
				info.i2_current_gop <- None;
				Mutex.unlock info_mutex;
*)

				Mutex.lock gop_tree_mutex;
				decr dishers_working_ref;
(*				gop_working_array.(index) <- None;*)
				change_working_job None; (* Hooray for two-mutex locking! *)
				Rbtree.add gop_tree (orig_ratio,gop.gop_number) gop;
				Mutex.unlock gop_tree_mutex;
				Unix.sleep timeout; (* Wait if something goes wrong *)
				close_connection sock send recv port
			)


		) and return_job sock send recv port compress orig_ratio gop target_bits optimal_bits requested_bits output_file_name = (
			let add_some_bits = (if gop.gop_number <> 0 then first_frame_extra_bits else 0) in
			let add_some_bits_float = float_of_int add_some_bits in
			let acheived_bits = float_of_int (Array.fold_left (fun so_far s ->
				so_far + s.stats_itex + s.stats_ptex + s.stats_mv + s.stats_misc
			) 0 gop.gop_frames) in
			print_log "got %f bits (requested %f, optimal %f)\n" acheived_bits target_bits optimal_bits;

			(* This is to save the previous file name for deletion after the GOP is sucessfully added back into the tree *)
			let old_output_file_name = gop.gop_output_file_name in

			gop.gop_attempted_bits <- Some (max 0.0 (target_bits -. add_some_bits_float), acheived_bits); (* Take out the SEI bits from target_bits before saving *)
			let new_ratio = min (optimal_bits /. acheived_bits) (acheived_bits /. optimal_bits) in
			gop.gop_output_file_name <- output_file_name;
			gop.gop_times_completed <- gop.gop_times_completed + 1;

			Mutex.lock gop_tree_mutex;
(*			gop_working_array.(index) <- None;*)
			change_working_job None;
			Rbtree.add gop_tree (new_ratio,gop.gop_number) gop;
			decr dishers_working_ref;
			incr gops_done_ref;
			expected_bits_so_far_ref := !expected_bits_so_far_ref +. optimal_bits;
			requested_bits_so_far_ref := !requested_bits_so_far_ref +. requested_bits;
			actual_bits_so_far_ref := !actual_bits_so_far_ref +. acheived_bits;
			print_log "added gop %d back at %f\n" gop.gop_number new_ratio;
			print_log "new mult is %f\n" (!requested_bits_so_far_ref /. !actual_bits_so_far_ref);

			print_log "expected bits:  %f\n" !expected_bits_so_far_ref;
			print_log "requested bits: %f\n" !requested_bits_so_far_ref;
			print_log "actual bits:    %f\n" !actual_bits_so_far_ref;
			print_log "-expected:      %f\n" (all_available_bits_float -. !expected_bits_so_far_ref);
			print_log "-requested:     %f\n" (all_available_bits_float -. !requested_bits_so_far_ref);
			print_log "-actual:        %f\n" (all_available_bits_float -. !actual_bits_so_far_ref);

			(* Update the fps *)
			second_pass_frames_done_ref := !second_pass_frames_done_ref + (Array.length gop.gop_frames);
			if orig_ratio = 0.0 then second_pass_frames_total_done_ref := !second_pass_frames_total_done_ref + (Array.length gop.gop_frames);
(*			Types.print "Done with %d frames, at %f fps total\n" !second_pass_frames_done_ref (float_of_int !second_pass_frames_done_ref /. (Sys.time () -. second_pass_start_time));*)

			(* Writing to the output file *)
			output_string out_stats_handle (Printf.sprintf "#GOP %d %d %d %f %f %d <%s>\n" gop.gop_number gop.gop_frame_offset (Array.length gop.gop_frames) (max 0.0 (target_bits -. add_some_bits_float)) acheived_bits !gops_done_ref output_file_name);
			Array.iter (fun x -> output_string out_stats_handle (Printf.sprintf "%s\n" (string_of_stats_line x))) gop.gop_frames;
			output_string out_stats_handle "#GOPEND\n";
			flush out_stats_handle; (* Should help resuming... *)

			Mutex.unlock gop_tree_mutex;
			Condition.broadcast gop_tree_condition;

			(* Now see if the old file needs deleting *)
			if not o.o_keeptemp && old_output_file_name <> "" && is_file (Filename.concat second_dir old_output_file_name) then (
				(* The old file is not being used any more. BALEETED! *)
				(try
					Sys.remove (Filename.concat second_dir old_output_file_name);
					print_log "deleted old file \"%s\"\n" old_output_file_name;
				with
					_ -> () (* Or not. Doesn't really matter. *)
				)
			);
			
			check_out_job sock send recv port compress


		) and close_connection sock send recv port = (
			print_log "close_connection\n";
			(try
				Unix.close sock
			with
				_ -> ()
			);
(*			update "Disconnected";*)
			update S_Disconnected;
			attempt_connection ()
		) in

		print_log "Attempting first connection\n";
		attempt_connection ();

		Mutex.lock agent_hash_mutex;
		print_log "setting a2_kill_ok\n";
		(snd (Hashtbl.find agent_hash (ip,port))).(n).a2_kill_ok <- true;
		Condition.signal agent_hash_dead_condition;
		Mutex.unlock agent_hash_mutex;

		print_log "exiting...\n";
(*		update "No jobs left; exited";*)
		update S_Exited;

	) in

	(*************************)
	(* RE-RATECONTROL THREAD *)
	(*************************)
	(* This thread re-calculates the ratecontrol every so often *)
	let rc_guts () = (
		let print_mutex = (fun a -> print ~name:("> RC ") ~screen:false a) in
		let print = (fun a -> print ~name:("RC ") a) in
		let print_log = (fun a -> Controllerinclude.print ~screen:false ~name:("RC ") a) in
		print_log "starting (id %d)\n" (Thread.id (Thread.self ()));
		
		let rerc = if o.o_ratecontrol_gops <= 0 then gops_total else o.o_ratecontrol_gops in
		print_log "will redo the ratecontrol every %d GOPS (%f%%)\n" rerc (100.0 *. float_of_int rerc /. float_of_int gops_total);
		
		Mutex.lock gop_tree_mutex;
		print_mutex "LOCKED GOP_TREE_MUTEX\n";
		let rec do_rc next = (
			let keep_going = (!gops_done_ref <= max_gops_to_do) && (
(*				let ((first_ratio,_),_) = Btree.first gop_tree in*)
				match Rbtree.first gop_tree with
				| Some ((first_ratio,_),_) -> first_ratio <= pass_threshold || !dishers_working_ref <> 0
				| None -> !dishers_working_ref <> 0
			) in
			if keep_going then (
				if !gops_done_ref >= next then (
					(* DO IT! *)
					print_log "calculating...\n";
					(* Add all the GOPs being worked on to a temporary GOP tree *)
					(* The temp tree is used because the ratios need to be re-calculated using the new gop_optimal_bits *)
					(* Note that the temp_tree is indexed by (gop_number,old_ratio); exactly the opposite of the regular gop_tree *)
(*					Btree.print_tree gop_tree (fun (ratio,number) -> Printf.sprintf "%4d @ %f" number ratio) (fun _ -> "GOP");*)
					Mutex.lock pm;
					Rbtree.kpretty_print (fun x -> Controllerinclude.print ~lock:false ~screen:false ~name:("RC ") "%s" x) gop_tree (fun (ratio,num) -> Printf.sprintf "%4d @ %f" num ratio) (fun gop -> Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_zones);
					Mutex.unlock pm;
					let temp_tree = Rbtree.create () in
					Rbtree.iter (fun (ratio,gop_number) gop -> Rbtree.add temp_tree (gop_number,ratio) gop) gop_tree;
(*
					Array.iter (function
						| None -> ()
						| Some gop -> (Rbtree.add temp_tree (gop.gop_number,42.0) gop)
					) gop_working_array;
*)

					(* ARG! Locking two mutexes at the same time! *)
					Mutex.lock agent_hash_mutex;
					Hashtbl.iter (fun _ (_, agent_array) ->
						Array.iter (function
							| {a2_current_job = Some gop} -> (Rbtree.add temp_tree (gop.gop_number,42.0) gop)
							| _ -> ()
						) agent_array
					) agent_hash;
					Mutex.unlock agent_hash_mutex;

					rc_gop_tree temp_tree;

					Mutex.lock agent_hash_mutex;
					Hashtbl.iter (fun _ (_, agent_array) ->
						Array.iter (function
							| {a2_current_job = Some gop} -> (Rbtree.remove temp_tree (gop.gop_number,42.0))
							| _ -> ()
						) agent_array
					) agent_hash;
					Mutex.unlock agent_hash_mutex;
(*
					Array.iter (function
						| None -> ()
						| Some gop -> (Rbtree.remove temp_tree (gop.gop_number,42.0))
					) gop_working_array;
*)
					(* Add the GOPs back to gop_tree *)
					Rbtree.clear gop_tree;
					Rbtree.iter (fun (gop_number,old_ratio) gop ->
						(* Calculate the new ratio based on the new gop_optimal_bits and the old gop_attempted_bits (if available) *)
						let want = gop.gop_optimal_bits in
						match gop.gop_attempted_bits with
						| None -> Rbtree.add gop_tree (old_ratio,gop_number) gop (* just use the old ratio *)
						| Some (_,got) -> Rbtree.add gop_tree ((min (got /. want) (want /. got)),gop_number) gop
					) temp_tree;
(*					Btree.print_tree gop_tree (fun (ratio,number) -> Printf.sprintf "%4d @ %f" number ratio) (fun _ -> "GOP");*)
					Mutex.lock pm;
					Rbtree.kpretty_print (fun x -> Controllerinclude.print ~lock:false ~screen:false ~name:("RC ") "%s" x) gop_tree (fun (ratio,num) -> Printf.sprintf "%4d @ %f" num ratio) (fun gop -> Printf.sprintf "#%03d@%06d %S" gop.gop_number gop.gop_frame_offset gop.gop_zones);
					Mutex.unlock pm;

					do_rc (next + rerc)
				) else (
					(* Not yet... *)
					print_mutex "WAITING ON GOP_TREE_CONDITION (%d < %d)\n" !gops_done_ref next;
					Condition.wait gop_tree_condition gop_tree_mutex;
					print_mutex "GOING ON GOP_TREE_CONDITION\n";
					do_rc next
				)
			) else (
				print_log "died!\n";
			)
		) in
		do_rc gops_total;
		print_mutex "UNLOCKED GOP_TREE_MUTEX\n";
		Mutex.unlock gop_tree_mutex;
	) in

(*	let dishing_threads = Array.mapi (fun i stuff -> Thread.create disher_guts (stuff,i)) o.o_agents_2 in*)

	(* Make a variable that will determine if the encoding is done *)
	(* There really should be a better way for determining this, but I don't know it yet *)
	let encoding_done_ref = ref false in
	let encoding_done_mutex = Mutex.create () in

	(***************)
	(* PING THREAD *)
	(***************)
	let ping_guts () = (
		(* Send out a signal every so often *)
		let print = (fun a -> print ~screen:false ~name:"PING2 " a) in
		print "PING!\n";
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
		let rec ping_loop () = (
			Mutex.lock encoding_done_mutex;
			let stop_working = !encoding_done_ref in
			Mutex.unlock encoding_done_mutex;
			if stop_working then (
				print "Encoding's done. Everybody go home.\n";
			) else (
				(* Keep going! *)
				print "Pinging %S\n" sendthis;
				if broadcast sendthis < String.length sendthis then (
					print "Oops. Didn't send all of the string. I wonder why...\n";
				);
				Thread.delay 60.0;
				ping_loop ()
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

		let print = (fun a -> print ~screen:false ~name:"DISCO2 " a) in
		
		Mutex.lock agent_hash_mutex;
		Array.iter (fun a ->
			let num_agents = snd a.ac_agents in
			let agents = Array.init num_agents (fun x -> {
				a2_current_job = None;
(*				a2_status = "Disconnected";*)
				a2_status = S_Disconnected;
				a2_total_frames = 0;
				a2_total_time = 0.0;
				a2_last_start = 0.0;
				a2_from_config = true;
				a2_kill_ok = false;
				a2_thread = Thread.create disher_guts (a.ac_name, (a.ac_ip, a.ac_port), x);
			}) in
			Hashtbl.add agent_hash (a.ac_ip, a.ac_port) (a.ac_name, agents)
		) o.o_agents;
		Mutex.unlock agent_hash_mutex;

		(* Check the agents for anything new *)
		let sock = Unix.socket Unix.PF_INET Unix.SOCK_DGRAM 0 in
		let rec keep_trying times = (try
			Unix.bind sock (Unix.ADDR_INET (Unix.inet_addr_any, o.o_adhoc_controller))
		with
			x -> (print "failed with %S\n" (Printexc.to_string x); if times <= 0 then raise x else (Thread.delay 10.0; keep_trying (pred times)))
		) in
		keep_trying 5;

		let recv_string = String.create 640 in (* 640 should be enough for anybody *)
		(try
			while true do
				(* I should find a way to stop this... later *)
				try
					let (got_bytes, from_addr) = Unix.recvfrom sock recv_string 0 (String.length recv_string) [] in
					Mutex.lock encoding_done_mutex;
					let no_more = !encoding_done_ref in 
					Mutex.unlock encoding_done_mutex;
					if no_more then (
						print "all agents are done\n";
						raise End_of_file;
					) else if got_bytes >= 24 && String.sub recv_string 0 4 = "AGNT" && Digest.substring recv_string 0 (got_bytes - 16) = String.sub recv_string (got_bytes - 16) 16 then (
						(* OK *)
						let agent_ip = match from_addr with
							| Unix.ADDR_INET (ip,port) -> Unix.string_of_inet_addr ip
							| _ -> "0.0.0.0" (* Why did this connect? Throw away! *)
						in
						let agent_port = unpackn recv_string 4 in
						let agent_1st = unpackC recv_string 6 in
						let agent_2nd = unpackC recv_string 7 in
						let agent_name = if got_bytes > 24 then String.sub recv_string 8 (got_bytes - 24) else (Printf.sprintf "%s:%d" agent_ip agent_port) in

						print "got response from %s:%d = (%d,%d) agents under name %S\n" agent_ip agent_port agent_1st agent_2nd agent_name;

						Mutex.lock agent_hash_mutex;
						if agent_ip <> "0.0.0.0" && not (Hashtbl.mem agent_hash (agent_ip, agent_port)) then (
							let agents = Array.init agent_2nd (fun x -> {
								a2_current_job = None;
(*								a2_status = "Disconnected";*)
								a2_status = S_Disconnected;
								a2_total_frames = 0;
								a2_total_time = 0.0;
								a2_last_start = 0.0;
								a2_from_config = false;
								a2_kill_ok = false;
								a2_thread = Thread.create disher_guts (agent_name, (agent_ip, agent_port), x);
							}) in
							Hashtbl.add agent_hash (agent_ip, agent_port) (agent_name, agents);
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
					Thread.delay 10.0; (* Just in case it keeps happening *)
				)
			done
		with
			End_of_file -> (print "exiting the discover thread\n")
		)
	) in
	let discover_thread_perhaps = if o.o_adhoc_enabled && o.o_adhoc_controller <> 0 then (
		Some (Thread.create discover_guts ())
	) else (
		None
	) in


	let rc_thread = Thread.create rc_guts () in
(*	Array.iter (fun x -> Thread.join x) dishing_threads;*)
(*	print ~screen:false "Dishers kicked the bucket\n";*)

	(* Kill off the dishers *)
	(* I don't think this works if there are no dishers defined, though... *)
	let rec kill_dishers_unsafe () = (
		print ~screen:false "KILL: kill_dishers_unsafe\n";
(*
		Mutex.unlock agent_hash_mutex;
		Thread.delay 10.0; (* HAX! *)
		Mutex.lock agent_hash_mutex;
		print ~screen:false "KILL: NOW kill the dishers\n";
*)
		print_agent_hash_pm ();
		let kill_all = if check_for_more_jobs () then (
			print ~screen:false "KILL: Still some jobs to do\n";
			false
		) else (
			Hashtbl.fold (fun _ (_,agent_array) so_far ->
				if so_far then (
					Array.fold_left (fun so_far {a2_kill_ok = x} -> so_far && x) true agent_array
				) else (
					(* Some dishers aren't dead yet. Don't bother to check all *)
					false
				)
			) agent_hash true
		) in
		print ~screen:false "KILL: kill all? %B\n" kill_all;
		if kill_all then (
			print ~screen:false "KILL: everything's done\n";
			(* DONE! *)
		) else (
			print ~screen:false "KILL: keep going (wait on agent_hash_dead_condition)\n";
			Condition.wait agent_hash_dead_condition agent_hash_mutex;
			print ~screen:false "KILL: waking up\n";
			kill_dishers_unsafe ()
		)
	) in
	Mutex.lock agent_hash_mutex;
	kill_dishers_unsafe ();
	Mutex.unlock agent_hash_mutex;

	(* Signal to the ping and discover threads that the encoding is done *)
	Mutex.lock encoding_done_mutex;
	encoding_done_ref := true;
	Mutex.unlock encoding_done_mutex;
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

	Thread.join rc_thread;
	print ~screen:false "RC kicked the bucket\n";

	close_out out_stats_handle;


	(***************)
	(* FINAL STATS *)
	(***************)
	let (final_attempt,final_actual) = Rbtree.fold_left (fun (attempt,actual) _ new_gop -> match new_gop.gop_attempted_bits with
		| None -> (attempt,actual) (* Uhhh... this shouldn't happen *)
		| Some (x,y) -> (attempt +. x, actual +. y)
	) (0.0,0.0) gop_tree in
	print ~screen:false "%f bits requested by user\n" all_available_bits_float;
	print ~screen:false "%f bits requested by agent\n" final_attempt;
	print ~screen:false "%f bits received from agents\n" final_actual;
	print ~screen:false "%.2f%%\n" (100. *. final_actual /. all_available_bits_float);
	print ~screen:false "%f final multiplier\n" (final_attempt /. final_actual);



	(* Put the filenames in order *)
	let final_gop_array = Array.make gops_total null_gop in
	Rbtree.iter (fun _ gop ->
		if is_file (Filename.concat second_dir gop.gop_output_file_name) then (
			final_gop_array.(gop.gop_number) <- gop
		) else (
			failwith (Printf.sprintf "ERROR: The file \"%s\" does not exist. This shouldn't happen.\n" (Filename.concat second_dir gop.gop_output_file_name));
(*			final_gop_array.(gop.gop_number) <- gop*)
		)
	) gop_tree;
	
	Array.iteri (fun j x -> print ~screen:false "#%04d: %S\n" j x.gop_output_file_name) final_gop_array;

	let timecode_scale_ref = ref 1000000L in (* This is for getting the timecode scale from the first file *)

	(* This helps the annoying "Fatal error: exception Sys_error("Bad file descriptor")" thing, but I STILL don't know why... *)
	Gc.full_major ();

	let out = open_out_bin o.o_output_file in
	Array.iteri (fun j gop ->
		print "Merging %d / %d\t\t\r" (succ j) gops_total;
		let name = Filename.concat second_dir gop.gop_output_file_name in
		if j = 0 then (
			(* First file! *)
			let first_override_list = [
				{t_name = "TimecodeScale"  ; t_id = 0x0AD7B1L; t_innards = T_uint_f (fun (id,len,innards) -> timecode_scale_ref := innards; [{elt_id = id; elt_len = len; elt_innards = In_uint innards}])};
				{t_name = "SegmentDuration"; t_id = 0x0489L  ; t_innards = T_float_f (fun (id,len,innards) ->
					let dur = Int64.to_float (1000000000L *| !| (i.i_num_frames) *| !| (i.i_fps_d)) /. Int64.to_float (!timecode_scale_ref *| !| (i.i_fps_n)) in
					[{elt_id = id; elt_len = 8L; elt_innards = In_float dur}]
				)}
			] in
			let m = parse_with_dtd name (List.rev_append (List.rev_append ebml_dtd matroska_dtd) first_override_list) in
			(if o.o_savedisk then try Sys.remove name with _ -> ());
			let rendered = render_elements m in
			List.iter (fun x ->
				output_string out x
			) rendered
		) else (
			
			(***************)
			(* REMOVE SEI! *)
			(***************)
			let first_frame_ref = ref true in
			let remove_sei_from_frame x = (match (x,!first_frame_ref) with
				| ([],_) -> [] (* Nothing to remove *)
				| ( _,false) -> x (* Not the first frame *)
				| (hd :: [], true) -> (
(*					print " First frame\n";*)
					first_frame_ref := false;
					let block = block_of_string hd in
					let frames = (match (block.block_lacing, block.block_frames) with
						| (Lacing_none, [f1]) -> (
(*							print "  No lacing\n";*)
							if (
								String.length f1 > 6 &&
								String.sub f1 0 2 = "\x00\x00" &&
								(f1.[2] = '\x01' || f1.[2] = '\x02' || f1.[2] = '\x03') &&
								String.sub f1 4 2 = "\x06\x05"
							) then (
(*								print "   Stuff checks out\n";*)
								let (len, len_len) = read_xiph_from_string f1 6 in
								let len = Int64.to_int len in
								let len_len = Int64.to_int len_len in
								if String.length f1 > 6 + len + len_len && len > 16 && String.sub f1 (6 + len_len) 16 = "\xDC\x45\xE9\xBD\xE6\xD9\x48\xB7\x96\x2C\xD8\x20\xD9\x23\xEE\xEF" then (
(*									print "    ID OK\n";*)
									[(String.sub f1 0 0) ^ (String.sub f1 (6 + len_len + len + 1) (String.length f1 - (6 + len_len + len + 1)))]
								) else (
									[f1]
								)
							) else (
								[f1]
							)
						)
						| _ -> block.block_frames
					) in
					block.block_frames <- frames;
					(string_of_block block) :: []
				)
				| _ -> (first_frame_ref := false; x) (* String's too big *)
			) in
			
			let beginning_timecode = Int64.of_float ((1000000000. *. (float_of_int gop.gop_frame_offset)) /. (Int64.to_float !timecode_scale_ref *. i.i_fps_f)) in
			let override_list = [
				{t_name = "EBML"    ; t_id = 0x0A45DFA3L; t_innards = T_skipped};
				{t_name = "Info"    ; t_id = 0x0549A966L; t_innards = T_skipped};
				{t_name = "Tracks"  ; t_id = 0x0654AE6BL; t_innards = T_skipped};
				{t_name = "Segment" ; t_id = 0x08538067L; t_innards = T_elt_f (fun (id,len,innards) -> innards)}; (* Flatten the segment *)
				{t_name = "Timecode"; t_id = 0x67L      ; t_innards = T_uint_f (fun (id,len,innards) ->
					let outards = write_uint_to_string (beginning_timecode +| innards) in
					[{elt_id = id; elt_len = !| (String.length outards); elt_innards = In_unparsed [outards]}]
				)};
				{t_name = "Block"   ; t_id = 0x21L      ; t_innards = T_binary_f (fun (id,len,innards) ->
					let out_binary = remove_sei_from_frame innards in
					let len = List.fold_left (fun s t -> s +| !| (String.length t)) 0L out_binary in
					[{elt_id = id; elt_len = len; elt_innards = In_binary out_binary}]
				)};
			] in
			let m = parse_with_dtd name (List.rev_append (List.rev_append ebml_dtd matroska_dtd) override_list) in
			
			(* Delete the file right now if the user says so *)
			(if o.o_savedisk && not o.o_keeptemp then try Sys.remove name with _ -> ());

			let rendered = render_elements m in
			List.iter (fun x ->
				output_string out x
			) rendered
		);
	) final_gop_array;
	print ~screen:false "\n";
	print ~screen:false "SECOND PASS DONE in %f seconds\n" (Sys.time () -. second_pass_start_time);
	close_out out;

	()
;;
(* LE END *)
