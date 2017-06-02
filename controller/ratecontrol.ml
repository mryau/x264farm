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

(* "modify the bitrate curve from pass1 for one frame" *)
let get_qscale o f rate_factor =
	(* blurred_complex ^ (1 - 0.6) *)
	match f.stats_zone with
	| Zone_bitrate x -> (f.stats_blurcomp ** (1.0 -. o.o_rc_qcomp)) /. (x *. rate_factor)
	| Zone_q       x -> qscale_of_qp_f x
;;

(* Make the I and B frames of a given GOP based on the P frames of that GOP *)
let get_diff_limited_q o gop total_macroblocks =
	let ip = o.o_rc_ipratio in
	let pb = o.o_rc_pbratio in
	let pb_ref = (pb +. 1.0) /. 2.0 in
	let min_qscale = qscale_of_qp_i o.o_rc_qpmin in
	let max_qscale = qscale_of_qp_i o.o_rc_qpmax in
	let qscale_mult_step = 2.0 ** (float_of_int o.o_rc_qpstep /. 6.0) in
	let qscale_div_step = 1.0 /. qscale_mult_step in
	let clamp_qscale x = max (min x max_qscale) min_qscale in
(*	let clamp_qscale_step last_qscale x = max (min (clamp_qscale x) (qscale_of_qp_i (last_qscale + o.o_rc_qpstep))) (qscale_of_qp_i (last_qscale - o.o_rc_qpstep)) in*)
	let clamp_qscale_step last_qscale x = max (max (min (min x (last_qscale *. qscale_mult_step)) max_qscale) (last_qscale *. qscale_div_step)) min_qscale in

	let frames = gop.gop_frames in
(* fold_left *)
(*	ignore (Array.fold_left (fun (last_p_qscale, last_p_or_i_qscale, last_non_b_frame, sum_p_qscale, sum_p_weight) new_frame ->*)
	ignore (
		Array.fold_right (fun new_frame (last_p_qscale, last_p_or_i_qscale, last_non_b_frame, sum_p_qscale, sum_p_weight) ->
			match new_frame.stats_type with
			| Stats_frame_I | Stats_frame_i -> (
				let iq = new_frame.stats_out_qscale in
				let pq = sum_p_qscale /. sum_p_weight in (* The x264 file uses the average of the QP converted to qscale, but the average of the qscale shouldn't be too far off *)
				if sum_p_weight <= 0.0 then (
					(* The next frame must have been an i frame as well *)
					(* Don't change anything *)
					new_frame.stats_out_qscale <- clamp_qscale_step last_p_or_i_qscale new_frame.stats_out_qscale;
				) else if sum_p_weight >= 1.0 then (
					(* A whole bunch of P frames? *)
					new_frame.stats_out_qscale <- pq /. ip;
					new_frame.stats_out_qscale <- clamp_qscale new_frame.stats_out_qscale; (* For some reason clamp_qscale_step is not done if sum_p_weight is too big... *)
				) else (
					(* Blend between the previous two options depending on sum_p_weight *)
					new_frame.stats_out_qscale <- sum_p_weight *. pq /. ip +. (1.0 -. sum_p_weight) *. iq;
					new_frame.stats_out_qscale <- clamp_qscale_step last_p_or_i_qscale new_frame.stats_out_qscale;
				);
(*				new_frame.stats_out_qscale <- clamp_qscale new_frame.stats_out_qscale;*)
				(last_p_qscale, new_frame.stats_out_qscale, Stats_frame_I, 0.0, 0.0)
			)
			| Stats_frame_B -> (
				(* Set the B frame to the qscale of the last non-B frame, but DON'T apply pb (since other frames may depend on it) *)
				(* Scratch that; it seems like applying a bit of pb to the frame is more like what x264 actually does *)
				new_frame.stats_out_qscale <- last_p_or_i_qscale *. pb_ref;
				new_frame.stats_out_qscale <- clamp_qscale new_frame.stats_out_qscale;
				(last_p_qscale, last_p_or_i_qscale, last_non_b_frame, sum_p_qscale, sum_p_weight)
			)
			| Stats_frame_b -> (
				(* Same as Frame_B, but apply pb *)
				new_frame.stats_out_qscale <- last_p_or_i_qscale *. pb;
				new_frame.stats_out_qscale <- clamp_qscale new_frame.stats_out_qscale;
				(last_p_qscale, last_p_or_i_qscale, last_non_b_frame, sum_p_qscale, sum_p_weight)
			)
			| Stats_frame_P -> (
				(* The big one! *)
				if new_frame.stats_itex + new_frame.stats_ptex = 0 && last_non_b_frame = Stats_frame_P then (
					(* The current frame has nothing in it, so just use the last P frame's qscale *)
					new_frame.stats_out_qscale <- last_p_qscale;
				);
				let mask = 1.0 -. (float_of_int new_frame.stats_itex /. float_of_int total_macroblocks) ** 2.0 in
(*				new_frame.stats_out_qscale <- clamp_qscale new_frame.stats_out_qscale;*)
				new_frame.stats_out_qscale <- clamp_qscale_step last_p_qscale new_frame.stats_out_qscale;
				(
					new_frame.stats_out_qscale,
					new_frame.stats_out_qscale,
					Stats_frame_P,
					mask *. (new_frame.stats_out_qscale +. sum_p_qscale),
					mask *. (1.0 +. sum_p_weight)
				)
			)
		)
		frames
		(
			qscale_of_qp_i 26,
			qscale_of_qp_i 26,
			Stats_frame_I,
			0.0,
			0.0
		)
	)
;;

(* This returns a multiplier for the bitrate *)
let monkeys_rate_estimate_qscale bits_so_far expected_bits_so_far bps rate_tolerance =
	let abr_buffer = 100.0 *. 2.0 *. rate_tolerance *. bps in
	let diff = bits_so_far -. expected_bits_so_far in
	(* Remember that multiplying the qscale DIVIDES the bitrate *)
	if bits_so_far < 1.0 then 1.0 else (
		(
			min 2.0 (max 0.5 ((abr_buffer -. diff) /. abr_buffer))
		) /. (
			bits_so_far /. expected_bits_so_far
		)
	)
;;

(* Big rate-control function *)
let rate_control_gop_tree gop_tree num_macroblocks o all_available_bits_float rf_mult rf_min_step =

	(* Complexity blur per GOP *)
	let blur_gop gop = (
		let frame_array = gop.gop_display_frames in
		(* Future blur *)
		let rec future_blur current_weight frame_blur_from keep_going weight_so_far comp_so_far = (
			if frame_blur_from >= Array.length frame_array || keep_going = 0 || current_weight < 0.0001 then (
				(weight_so_far, comp_so_far)
			) else (
				let f = frame_array.(frame_blur_from) in
				let new_weight = current_weight *. (1.0 -. (float_of_int f.stats_imb /. float_of_int num_macroblocks) ** 2.0) in
				let new_weight_so_far = weight_so_far +. new_weight in
				let new_comp_so_far = comp_so_far +. new_weight *. f.stats_comp in
				future_blur new_weight (succ frame_blur_from) (pred keep_going) new_weight_so_far new_comp_so_far
			)
		) in
		(* Past blur *)
		let rec past_blur current_weight frame_blur_from keep_going weight_so_far comp_so_far = (
			if frame_blur_from < 0 || keep_going = 0 || current_weight < 0.001 then (
				(weight_so_far, comp_so_far)
			) else (
				let f = frame_array.(frame_blur_from) in
				let new_weight_so_far = weight_so_far +. current_weight in
				let new_comp_so_far = comp_so_far +. current_weight *. f.stats_comp in
				let new_weight = current_weight *. (1.0 -. (float_of_int f.stats_imb /. float_of_int num_macroblocks) ** 2.0) in
				past_blur new_weight (pred frame_blur_from) (pred keep_going) new_weight_so_far new_comp_so_far
			)
		) in
		Array.iteri (fun frame_i frame_to ->
			(* Blur frames in GOP *)
			let (weight_future, comp_future) = future_blur 1.0 (frame_i + 1) (int_of_float (o.o_rc_cplxblur *. 2.0)) 0.0 0.0 in
			let (weight_past  , comp_past  ) =   past_blur 1.0 frame_i (int_of_float (o.o_rc_cplxblur *. 2.0)) 0.0 0.0 in
			let blurred_comp = (if weight_past +. weight_future = 0.0 then 0.0 else (comp_future +. comp_past) /. (weight_future +. weight_past)) in
			frame_to.stats_blurcomp <- blurred_comp;
		) frame_array;
	) in
	(*List2.iter blur_gop gop_list;*)
	Rbtree.iter (fun _ gop -> blur_gop gop) gop_tree;

	(* Find the number of bits expected at qscale=1 *)
	let rate_factor_1_bits = Rbtree.fold_left (fun gop_so_far _ gop_now ->
		let bits_in_gop = Array.fold_left (fun frame_so_far frame_now ->
			frame_so_far +. bits_of_qscale frame_now (get_qscale o frame_now 1.0)
		) 0.0 gop_now.gop_display_frames in
		gop_so_far +. bits_in_gop
	) 1.0 gop_tree in
	print ~screen:false "Avail:   %f\n" all_available_bits_float;
	print ~screen:false "SP exp:  %f\n" rate_factor_1_bits;
	let step_mult = all_available_bits_float /. rate_factor_1_bits in
	let filter_size = int_of_float (o.o_rc_qblur *. 4.0) lor 1 in

	(****************************************)
	(* NEW linear (not binary) based search *)
	(****************************************)
	let rate_control rate_factor =
		(* Find qscale of each frame at the current rate_factor *)
		Rbtree.iter (fun _ gop ->
			Array.iter (fun f -> f.stats_out_qscale <- get_qscale o f rate_factor) gop.gop_display_frames
		) gop_tree;

		(* Tweak the frame quantizers *)
		Rbtree.iter (fun _ gop_now ->
			get_diff_limited_q o gop_now num_macroblocks;
		) gop_tree;

		(* Smooth "curve" *)
		(* A bit different since it's now per-GOP, but that shouldn't matter too much. It only blurs 3 frames together anyway. *)
		Rbtree.iter (fun _ gop ->
			let flat_display_array = gop.gop_display_frames in
			Array.iteri (fun i_update frame_update ->
				let q_ref = ref 0.0 in
				let sum_ref = ref 0.0 in
				for j_offset = 0 to filter_size - 1 do
					let j_add = i_update + j_offset - filter_size / 2 in
					let delta = float_of_int (j_offset - filter_size / 2) in
					let coeff = exp (~-. delta *. delta /. (o.o_rc_qblur *. o.o_rc_qblur)) in
					if j_add < 0 || j_add >= Array.length gop.gop_display_frames then () else (
						if generalize_frame_types frame_update.stats_type <> generalize_frame_types flat_display_array.(j_add).stats_type then () else (
							(* Only use frames which are the same type as the output frame *)
							q_ref := !q_ref +. flat_display_array.(j_add).stats_out_qscale *. coeff;
							sum_ref := !sum_ref +. coeff
						)
					)
				done;
				frame_update.stats_out_blurqscale <- !q_ref /. !sum_ref;
			) gop.gop_display_frames
		) gop_tree;

		(* Expected bits *)
		let expected_bits = Rbtree.fold_left (fun gop_so_far _ gop ->
			let bits_in_gop = Array.fold_left (fun frame_so_far f ->
				frame_so_far +. bits_of_qscale f f.stats_out_blurqscale
			) 0.0 gop.gop_display_frames in
			gop_so_far +. bits_in_gop
		) 0.0 gop_tree in

		print ~screen:false "Expect %f bits with ratefactor %-9.4f\n" expected_bits rate_factor;

		expected_bits
	in

	let off_by_bits = 1.0 in (* Error in the number of bits before it's "done enough" *)
	let rec do_step (rf1,bits1) (rf2,bits2) more_steps =
		(* New 20070424 *)
		if bits2 <= bits1 +. 0.001 *. (rf2 -. rf1) then (
			(* The bitrate has encountered the max! Adding more bits will do nothing to raise the bitrate, and it will make bad guesses for lowering the bitrate *)
			if bits2 < all_available_bits_float then (
				(* Won't reach it *)
				print ~screen:false "BITRATE FLATLINE AT %.0f. Decrease --qpmin or the target bitrate to get an accurate result\n" bits1;
				rf2
			) else (
				let rf_half = rf1 /. 2.0 in
				let bits_half = rate_control rf_half in
				if more_steps = 0 then (
					print ~screen:false "FLATLINE and no more steps! Everything is wrong!\n";
					rf_half
				) else (
					print ~screen:false "FLATLINE! Cutting the min ratefactor in half to %.0f\n" rf_half;
					do_step (rf_half,bits_half) (rf1,bits1) (pred more_steps)
				)
			)
		) else (
		(* !New 20070424 *)

			let rate_factor = max 0.0 ((bits1 *. rf2 -. bits2 *. rf1 +. all_available_bits_float *. (rf1 -. rf2)) /. (bits1 -. bits2)) in
			print ~screen:false "Chose %.4f from (%.4f,%.0f) to (%.4f,%.0f) (%d steps left)\n" rate_factor rf1 bits1 rf2 bits2 more_steps;

			let expected_bits = rate_control rate_factor in

			if abs_float (expected_bits -. all_available_bits_float) < off_by_bits then (
				(* Good enough! *)
				rate_factor
			) else if more_steps = 0 then (
				print ~screen:false "WARNING: 2pass curve failed to converge\n";
				rate_factor
			) else if rate_factor > rf2 then (
				(* The max ratefactor was too small; add the current one at the end *)
				do_step (rf2,bits2) (rate_factor,expected_bits) (pred more_steps)
			) else if rate_factor < rf1 then (
				(* Min ratefactor was too big *)
				do_step (rate_factor,expected_bits) (rf1,bits1) (pred more_steps)
			) else if expected_bits > all_available_bits_float then (
				(* Ratefactor too big, replace the upper bound *)
				do_step (rf1,bits1) (rate_factor,expected_bits) (pred more_steps)
			) else (
				(* Ratefactor too small, replace the lower bound *)
				do_step (rate_factor,expected_bits) (rf2,bits2) (pred more_steps)
			)
		)
	in

	(* Find a good starting value for the ratefactor *)
	let step_mult_bits = rate_control step_mult in
	let min_bits = rate_control 0.0 in

	do_step (0.0,min_bits) (step_mult,step_mult_bits) 32
;;




(* Same as rate_control_gop_tree, but only for one GOP object *)
let rate_control_gop gop use_prev_rf_start_point num_macroblocks o all_available_bits_float rf_mult rf_min_step =

	(
		let frame_array = gop.gop_display_frames in
		(* Future blur *)
		let rec future_blur current_weight frame_blur_from keep_going weight_so_far comp_so_far = (
			if frame_blur_from >= Array.length frame_array || keep_going = 0 || current_weight < 0.0001 then (
				(weight_so_far, comp_so_far)
			) else (
				let f = frame_array.(frame_blur_from) in
				let new_weight = current_weight *. (1.0 -. (float_of_int f.stats_imb /. float_of_int num_macroblocks) ** 2.0) in
				let new_weight_so_far = weight_so_far +. new_weight in
				let new_comp_so_far = comp_so_far +. new_weight *. f.stats_comp in
				future_blur new_weight (succ frame_blur_from) (pred keep_going) new_weight_so_far new_comp_so_far
			)
		) in
		(* Past blur *)
		let rec past_blur current_weight frame_blur_from keep_going weight_so_far comp_so_far = (
			if frame_blur_from < 0 || keep_going = 0 || current_weight < 0.001 then (
				(weight_so_far, comp_so_far)
			) else (
				let f = frame_array.(frame_blur_from) in
				let new_weight_so_far = weight_so_far +. current_weight in
				let new_comp_so_far = comp_so_far +. current_weight *. f.stats_comp in
				let new_weight = current_weight *. (1.0 -. (float_of_int f.stats_imb /. float_of_int num_macroblocks) ** 2.0) in
				past_blur new_weight (pred frame_blur_from) (pred keep_going) new_weight_so_far new_comp_so_far
			)
		) in
		Array.iteri (fun frame_i frame_to ->
			(* Blur frames in GOP *)
			let (weight_future, comp_future) = future_blur 1.0 (frame_i + 1) (int_of_float (o.o_rc_cplxblur *. 2.0)) 0.0 0.0 in
			let (weight_past  , comp_past  ) =   past_blur 1.0 frame_i (int_of_float (o.o_rc_cplxblur *. 2.0)) 0.0 0.0 in
			let blurred_comp = (if weight_past +. weight_future = 0.0 then 0.0 else (comp_future +. comp_past) /. (weight_future +. weight_past)) in
			frame_to.stats_blurcomp <- blurred_comp;
		) frame_array;
	);

	(* Find the number of bits expected at qscale=1 *)
	let rate_factor_1_bits = Array.fold_left (fun frame_so_far frame_now ->
		frame_so_far +. bits_of_qscale frame_now (get_qscale o frame_now 1.0)
	) 0.0 gop.gop_display_frames in
	print ~screen:false "Avail:   %f\n" all_available_bits_float;
	print ~screen:false "SP exp:  %f\n" rate_factor_1_bits;
	let filter_size = int_of_float (o.o_rc_qblur *. 4.0) lor 1 in

	(****************************************)
	(* NEW linear (not binary) based search *)
	(****************************************)
	let rate_control rate_factor =
		(* Find qscale of each frame at the current rate_factor *)
		Array.iter (fun f -> f.stats_out_qscale <- get_qscale o f rate_factor) gop.gop_display_frames;

		(* Tweak the frame quantizers *)
		get_diff_limited_q o gop num_macroblocks;

		(* Smooth "curve" *)
		(* A bit different since it's now per-GOP, but that shouldn't matter too much. It only blurs 3 frames together anyway. *)
		let flat_display_array = gop.gop_display_frames in
		Array.iteri (fun i_update frame_update ->
			let q_ref = ref 0.0 in
			let sum_ref = ref 0.0 in
			for j_offset = 0 to filter_size - 1 do
				let j_add = i_update + j_offset - filter_size / 2 in
				let delta = float_of_int (j_offset - filter_size / 2) in
				let coeff = exp (~-. delta *. delta /. (o.o_rc_qblur *. o.o_rc_qblur)) in
				if j_add < 0 || j_add >= Array.length gop.gop_display_frames then () else (
					if generalize_frame_types frame_update.stats_type <> generalize_frame_types flat_display_array.(j_add).stats_type then () else (
						(* Only use frames which are the same type as the output frame *)
						q_ref := !q_ref +. flat_display_array.(j_add).stats_out_qscale *. coeff;
						sum_ref := !sum_ref +. coeff
					)
				)
			done;
			frame_update.stats_out_blurqscale <- !q_ref /. !sum_ref;
		) gop.gop_display_frames;

		(* Expected bits *)
		let expected_bits = Array.fold_left (fun frame_so_far f ->
			frame_so_far +. bits_of_qscale f f.stats_out_blurqscale
		) 0.0 gop.gop_display_frames in

		print ~screen:false "Expect %f bits with ratefactor %-9.4f\n" expected_bits rate_factor;

		expected_bits
	in

	let off_by_bits = 1.0 in (* Error in the number of bits before it's "done enough" *)
	let rec do_step (rf1,bits1) (rf2,bits2) more_steps =
		(* New 20070424 *)
		if bits2 <= bits1 +. 0.001 *. (rf2 -. rf1) then (
			(* The bitrate has encountered the max! Adding more bits will do nothing to raise the bitrate, and it will make bad guesses for lowering the bitrate *)
			if bits2 < all_available_bits_float then (
				(* Won't reach it *)
				print ~screen:false "BITRATE FLATLINE AT %.0f. Decrease --qpmin or the target bitrate to get an accurate result\n" bits1;
				rf2
			) else (
				let rf_half = rf1 /. 2.0 in
				let bits_half = rate_control rf_half in
				do_step (rf_half,bits_half) (rf1,bits1) (pred more_steps)
			)
		) else (
		(* !New 20070424 *)

			let rate_factor = max 0.0 ((bits1 *. rf2 -. bits2 *. rf1 +. all_available_bits_float *. (rf1 -. rf2)) /. (bits1 -. bits2)) in
			print ~screen:false "Chose %.4f from (%.4f,%.0f) to (%.4f,%.0f) (%d steps left)\n" rate_factor rf1 bits1 rf2 bits2 more_steps;

			let expected_bits = rate_control rate_factor in

			if abs_float (expected_bits -. all_available_bits_float) < off_by_bits then (
				(* Good enough! *)
				rate_factor
			) else if more_steps = 0 then (
				print ~screen:false "WARNING: 2pass curve failed to converge\n";
				rate_factor
			) else if rate_factor > rf2 then (
				(* The max ratefactor was too small; add the current one at the end *)
				do_step (rf2,bits2) (rate_factor,expected_bits) (pred more_steps)
			) else if rate_factor < rf1 then (
				(* Min ratefactor was too big *)
				do_step (rate_factor,expected_bits) (rf1,bits1) (pred more_steps)
			) else if expected_bits > all_available_bits_float then (
				(* Ratefactor too big, replace the upper bound *)
				do_step (rf1,bits1) (rate_factor,expected_bits) (pred more_steps)
			) else (
				(* Ratefactor too small, replace the lower bound *)
				do_step (rate_factor,expected_bits) (rf2,bits2) (pred more_steps)
			)
		)
	in

	(* Find a good starting value for the ratefactor *)
	let start_point = (
(*
		if use_prev_rf_start_point then (
			gop.gop_current_ratefactor
		) else (
*)
			let step_mult = all_available_bits_float /. rate_factor_1_bits in
			(step_mult, rate_control step_mult)
(*
		)
*)
	) in
	let min_bits = rate_control 0.0 in

	do_step (0.0,min_bits) start_point 32
;;
