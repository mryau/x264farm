Printf.printf "\n\n\n\n";;

(* TYPES *)
type frame_t = Frame_I of string | Frame_notI of string | No_frame;;
type range_type_t =
	| Range_empty_lobster
	| Range_full_lobster of frame_t array (* Stats for the range's frames *)
	| Range_queued_lobster
	| Range_working_lobster of (string) (* Worker name *)
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

let make_point   n = {range_prev = None; range_next = None; range_delfirst = false; range_frame = n; range_thresh = infinity};;
let make_point_d n = {range_prev = None; range_next = None; range_delfirst = true;  range_frame = n; range_thresh = infinity};;

let add_range_lobster p1 range_type p2 =
	let r = {range_type = range_type; range_left = p1; range_right = p2} in
	p1.range_next <- Some r;
	p2.range_prev <- Some r;
;;

let add_range_and_return_lobster p1 range_type p2 =
	let r = {range_type = range_type; range_left = p1; range_right = p2} in
	p1.range_next <- Some r;
	p2.range_prev <- Some r;
	r
;;

let print_log = Printf.printf;;

let print_range_width = 3;;
let rec print_ranges point = (
	let prev_ok = match point.range_prev with
		| None -> true
		| Some x -> x.range_right == point
	in
	let next_ok = match point.range_next with
		| None -> true
		| Some x -> x.range_left == point
	in
	print_log " %*d @ %s%s %s%s\n" print_range_width point.range_frame (match classify_float point.range_thresh with | FP_infinite -> "INFINITY" | _ -> Printf.sprintf "%.2f" point.range_thresh) (if point.range_delfirst then " INCORRECT" else "") (if prev_ok then "o" else "X") (if next_ok then "o" else "X");
	match point with
	| {range_next = Some r} -> (
		print_log " %*s %s\n" print_range_width "" (
			match r.range_type with
			| Range_empty_lobster -> "Empty"
			| Range_full_lobster a -> ("Full" ^ (Array.fold_left (fun s -> function | No_frame -> " >>NOFRAME<<" | _ -> s) " ok" a) ^ (if Array.length a = r.range_right.range_frame - r.range_left.range_frame then " ok" else " >>BADLENGTH<<"))
			| Range_queued_lobster -> "Queued"
			| Range_working_lobster s -> "Working " ^ s
		);
		print_ranges r.range_right
	)
	| _ -> () (* Done! *)
);;

let out_stats_handle = stderr;;

let _ = (

	let p0 = make_point 0 in
	let p1 = make_point_d 100 in
	let p2 = make_point 200 in
	let p3 = make_point 300 in
	let p4 = make_point 400 in

(*
	add_range_lobster p0 (Range_full_lobster (Array.make (p1.range_frame - p0.range_frame) (Frame_notI ""))) p1;
	add_range_lobster p0 (Range_working_lobster "?") p1;
	let working_range = add_range_and_return_lobster p1 (Range_working_lobster "ME!") p2 in
	add_range_lobster p2 (Range_full_lobster (Array.make (p3.range_frame - p2.range_frame) (Frame_I ""))) p3;
	add_range_lobster p2 (Range_working_lobster "other") p3;
	add_range_lobster p3 Range_empty_lobster p4;
*)

	add_range_lobster p0 (Range_full_lobster (Array.make (p1.range_frame - p0.range_frame) (Frame_notI ""))) p1;
(*	add_range_lobster p0 Range_empty_lobster p1;*)
	let working_range = add_range_and_return_lobster p1 (Range_working_lobster "ME!") p2 in
(*	add_range_lobster p1 (Range_full_lobster (Array.make (p3.range_frame - p2.range_frame) (Frame_notI ""))) p2;*)
(*	add_range_lobster p2 (Range_full_lobster (Array.make (p3.range_frame - p2.range_frame) (Frame_notI ""))) p3;*)
(*	add_range_lobster p3 Range_queued_lobster p4;*)
(*	add_range_lobster p2 (Range_working_lobster "?") p3;*)
	add_range_lobster p2 Range_queued_lobster p3;

(*	let working_range = add_range_and_return_lobster p0 (Range_working_lobster "me2") p1 in*)

	let i_num_frames = (
		match working_range.range_right.range_next with
		| None -> working_range.range_right.range_frame
		| Some _ -> working_range.range_right.range_frame + 200
	) in


	print_ranges p0;

	let is_last_range = (working_range.range_right.range_frame = i_num_frames) in
	let a = working_range.range_left.range_frame in                                              (* The frame to start encoding from (and therefore the start frame of the output array) *)
	let b = if is_last_range then i_num_frames - 1 else working_range.range_right.range_frame in (* The frame to end encoding *)
	let first_output_frame = a in                                                                (* The first frame that should be outputted to the range list *)
	let last_output_frame = if is_last_range then b else b - 1 in                                (* Last frame to be outputted. The frame at location b is expected to be an I frame, so it may be omitted *)
	let num_frames = b - a + 1 in
	
	let temp_stats_array = Array.make (last_output_frame - a + 1) (Frame_I "") in
	
	let put_range_back_unsafe_lobster first_i_frame last_i_frame range = (
		print_log "PUT BACK RANGE\n";

		let first_index_in_range = first_output_frame - a in
		let last_index_in_range = last_output_frame - a in
		let first_index_to_copy = if range.range_left.range_delfirst then first_i_frame else first_index_in_range in
		let last_index_to_copy = last_i_frame - 1 in

		range.range_left.range_delfirst <- false; (* Reset DELFIRST *)
		
		(* This function normalizes the working range with the previous range *)
		let (prev_range_perhaps, first_valid_point) = (
			match (range.range_left.range_prev, first_index_to_copy = first_index_in_range) with
			| (Some ({range_type = Range_full_lobster _} as r), true) -> (
				(* OK *)
				print_log "merging proper start with previous full range\n";
				(Some r, range.range_left)
			)
			| (_, true) -> (
				(* OK *)
				print_log "appending range to some other range type\n";
				(None, range.range_left)
			)
			| (Some {range_type = Range_empty_lobster; range_left = p1}, false) -> (
				(* OK *)
				print_log "adding first frames to previous empty range\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
				add_range_lobster p1 Range_empty_lobster new_point;
				(None, new_point)
			)
			| (Some {range_type = Range_queued_lobster; range_left = p1}, false) -> (
				(* OK *)
				print_log "adding first frames to previous queued range\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
				add_range_lobster p1 Range_queued_lobster new_point;
				(None, new_point)
			)
			| (_, false) -> (
				(* OK *)
				print_log "adding empty range after some sort of other range\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" a (a + first_index_to_copy - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + first_index_to_copy; range_thresh = infinity} in
				add_range_lobster range.range_left Range_empty_lobster new_point;
				(None, new_point)
			)
		) in
		(* This function normalizes the working range with the next range *)
		let (next_range_perhaps, last_valid_point, use_all) = (
			match (range.range_right.range_next, last_index_to_copy = last_index_in_range) with
			| (Some ({range_type = Range_full_lobster _} as r), true) -> (
				(* OK *)
				print_log "merging proper end with next full range\n";
				(Some r, range.range_right, false)
			)
			| (_, true) -> (
				(* OK *)
				print_log "prepending range before some other range type\n";
				(None, range.range_right, false)
			)
			| (Some {range_type = Range_empty_lobster; range_right = p2}, false) -> (
				(* OK *)
				print_log "adding last frames to next empty range\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
				add_range_lobster new_point Range_empty_lobster p2;
				(None, new_point, false)
			)
			| (Some {range_type = Range_queued_lobster; range_right = p2}, false) -> (
				(* OK *)
				print_log "adding last frames to next queued range\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
				add_range_lobster new_point Range_queued_lobster p2;
				(None, new_point, false)
			)
			| (Some {range_type = Range_working_lobster _}, false) -> (
				(* DELFIRST OK *)
				print_log "next is working; set to DELFIRST\n";
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (b - 1);
				let new_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = a + last_index_to_copy + 1; range_thresh = infinity} in
				add_range_lobster new_point Range_empty_lobster range.range_right;
				range.range_right.range_delfirst <- true;
				(None, new_point, false)
			)
			| (Some {range_type = Range_full_lobster full_array; range_right = point_after_full}, false) -> (
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
				let i_frame_option = find_first_i_frame 0 in
				match (i_frame_option, point_after_full.range_next) with
				| (Some x, _) -> (
					(* OK *)
					print_log "  chunk removed; fixing ranges\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (range.range_right.range_frame + x);
					let new_last_point = {range_prev = None; range_next = None; range_delfirst = false; range_frame = range.range_right.range_frame + x + 1; range_thresh = infinity} in
					add_range_lobster new_first_point Range_empty_lobster new_last_point;
					add_range_lobster new_last_point (Range_full_lobster (Array.sub full_array (x + 1) (len - x - 1))) point_after_full;
					(None, new_first_point, false)
				)
				| (None, Some {range_type = Range_empty_lobster; range_right = very_far_away}) -> (
					(* OK *)
					print_log "  removing full range and extending next empty range\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
					add_range_lobster new_first_point Range_empty_lobster very_far_away;
					(None, new_first_point, false)
				)
				| (None, Some {range_type = Range_queued_lobster; range_right = very_far_away}) -> (
					(* OK *)
					print_log "  removing full range and extending next queued range\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
					add_range_lobster new_first_point Range_queued_lobster very_far_away;
					(None, new_first_point, false)
				)
				| (None, Some {range_type = Range_working_lobster _}) -> (
					(* OK *)
					print_log "  removing full range and putting in an empty range, and setting point after that to DELFIRST\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
					add_range_lobster new_first_point Range_empty_lobster point_after_full;
					point_after_full.range_delfirst <- true;
					(None, new_first_point, false)
				)
				| (None, _) -> (
					(* OK *)
					print_log "  removing full range and putting in an empty range from %d to %d\n" new_first_point.range_frame point_after_full.range_frame;
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" (a + last_index_to_copy + 1) (point_after_full.range_frame - 1);
					add_range_lobster new_first_point Range_empty_lobster point_after_full;
					(None, new_first_point, false)
				)
			)
			| (None, false) -> (
				(* OK *)
				print_log "last range; set all end frames to done\n";
				(None, range.range_right, true)
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
		print_log "first_i_frame       = %d\n" first_i_frame;
		print_log "last_i_frame        = %d\n" last_i_frame;
		print_log "first_index_copy    = %d\n" first_index_to_copy;
		print_log "last_index_copy     = %d\n" last_index_to_copy;
		print_log "\n";
		let last_index_to_copy = (if use_all then last_index_in_range else last_index_to_copy) in
		print_log "modified last index = %d\n" last_index_to_copy;
		print_log "any good            = %B\n" (last_index_to_copy >= first_index_to_copy);

		match (prev_range_perhaps, next_range_perhaps) with
		| (Some {range_type = Range_full_lobster prev; range_left = p1}, Some {range_type = Range_full_lobster next; range_right = p2}) -> (
			(* OK *)
			print_log "merging three full ranges together\n";
			let a = Array.make (p2.range_frame - p1.range_frame) No_frame in
			Array.blit prev 0 a 0 (Array.length prev);
			Array.blit temp_stats_array first_index_to_copy a (first_valid_point.range_frame - p1.range_frame) (last_index_to_copy - first_index_to_copy + 1);
			Array.blit next 0 a (last_valid_point.range_frame - p1.range_frame) (Array.length next);
			add_range_lobster p1 (Range_full_lobster a) p2;
		)
		| (Some {range_type = Range_full_lobster prev; range_left = p1}, _) -> (
			(* OK *)
			print_log "merging range with its previous full range\n";
			let a = Array.make (last_valid_point.range_frame - p1.range_frame) No_frame in
			Array.blit prev 0 a 0 (Array.length prev);
			Array.blit temp_stats_array first_index_to_copy a (first_valid_point.range_frame - p1.range_frame) (last_index_to_copy - first_index_to_copy + 1);
			add_range_lobster p1 (Range_full_lobster a) last_valid_point;
		)
		| (_, Some {range_type = Range_full_lobster next; range_right = p2}) -> (
			(* OK *)
			print_log "merging range with its next full range\n";
			let a = Array.make (p2.range_frame - first_valid_point.range_frame) No_frame in
			Array.blit temp_stats_array first_index_to_copy a 0 (last_index_to_copy - first_index_to_copy + 1);
			Array.blit next 0 a (last_valid_point.range_frame - first_valid_point.range_frame) (Array.length next);
			add_range_lobster first_valid_point (Range_full_lobster a) p2;
		)
		| _ -> (
			(* OK *)
			print_log "no merging\n";
			let a = Array.sub temp_stats_array first_index_to_copy (last_index_to_copy - first_index_to_copy + 1) in
			add_range_lobster first_valid_point (Range_full_lobster a) last_valid_point;
		)
	) in



	let reset_range_unsafe_lobster () range = (

		print_log "RESET RANGE\n";

		range.range_left.range_delfirst <- false; (* Reset beginning DELFIRST *)
		
		(* This function fixes up the right side of the range so that DELFIRST gets set properly *)
		(
			match range.range_right.range_next with
			| (Some {range_type = Range_working_lobster _}) -> (
				(* DELFIRST OK *)
				Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (range.range_right.range_frame - 1);
				print_log "next is working; set to DELFIRST\n";
				range.range_right.range_delfirst <- true;
			)
			| (Some {range_type = Range_full_lobster full_array; range_right = point_after_full}) -> (
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
					add_range_lobster range.range_right Range_empty_lobster new_last_point;
					add_range_lobster new_last_point (Range_full_lobster (Array.sub full_array (x + 1) (len - x - 1))) point_after_full;
				)
				| (None, Some {range_type = Range_empty_lobster; range_right = very_far_away}) -> (
					(* OK *)
					print_log "  removing full range and extending next empty range\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
					add_range_lobster range.range_right Range_empty_lobster very_far_away;
				)
				| (None, Some {range_type = Range_queued_lobster; range_right = very_far_away}) -> (
					(* OK *)
					print_log "  removing full range and extending next queued range\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
					add_range_lobster range.range_right Range_queued_lobster very_far_away;
				)
				| (None, Some {range_type = Range_working_lobster _}) -> (
					(* OK *)
					print_log "  removing full range and putting in an empty range, and setting point after that to DELFIRST\n";
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
					add_range_lobster range.range_right Range_empty_lobster point_after_full;
					point_after_full.range_delfirst <- true;
				)
				| (None, _) -> (
					(* OK *)
					print_log "  removing full range and putting in an empty range from %d to %d\n" range.range_right.range_frame point_after_full.range_frame;
					Printf.fprintf out_stats_handle "#DELETE %d %d\n" range.range_left.range_frame (point_after_full.range_frame - 1);
					add_range_lobster range.range_right Range_empty_lobster point_after_full;
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

		match (range.range_left.range_prev, range.range_right.range_next) with
		| (Some {range_type = Range_empty_lobster; range_left = left}, Some {range_type = Range_empty_lobster; range_right = right}) -> (
			(* OK *)
			print_log "combining three empty ranges\n";
			add_range_lobster left Range_empty_lobster right;
		)
		| (Some {range_type = Range_empty_lobster; range_left = left}, _) -> (
			(* OK *)
			print_log "combining previous empty range\n";
			add_range_lobster left Range_empty_lobster range.range_right;
		)
		| (_, Some {range_type = Range_empty_lobster; range_right = right}) -> (
			(* OK *)
			print_log "combining next empty range\n";
			add_range_lobster range.range_left Range_empty_lobster right;
		)
		| _ -> (
			(* OK *)
			print_log "just hack in an empty range\n";
			add_range_lobster range.range_left Range_empty_lobster range.range_right;
		)
	) in



















	reset_range_unsafe_lobster () working_range;
(*	put_range_back_unsafe_lobster 5 15 working_range;*)

	print_ranges p0;

);;

Printf.printf "\n\n\n\n";;
