				if found_no_frame then (
					(* Nothing worthwhile happened, or the job didn't finish somehow *)
					
					Mutex.lock range_list_mutex;
					print_log "returning range (%d,%d) to Range_empty\n" a_job b_job;
					range_list_ref := reset_range_unsafe !range_list_ref;
print_ranges "put back range\n";
					Condition.broadcast range_list_full_condition;
					Mutex.unlock range_list_mutex;
				) else if last_i_frame = 0 then (
					(* There was only one I frame, so the whole range is no good. Let's hope there's nothing being worked on after it, or it will just be returned again *)
					
					let rec return_range_into_next_range = function
						| RangeRange_working a :: Range_full (b,c,full) :: tl when (a_job,b_job) = a -> (
							(* Take the first few frames from the range_full and put it into the current range *)
							let first_i_frame = find_first_i_frame full 0 in
							let first_i_frame_number = first_i_frame + b in
							if first_i_frame >= 
				) else (
					(* Something actually happened! *)
					print "something actually happened!\n";
					let start_offset = if a_job = 0 then 0 else 1 in
					let num_frames = last_i_frame - start_offset + 1 in
					let last_i_frame_number = last_i_frame + a in
					
					let put_range_back_unsafe = (
						if last_i_frame = b - a then (
							let rec rec_helper = function (* The whole thing was done *)
								| Range_full (a,b,full1) :: Range_working c :: Range_full (d,e,full2) :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (e - a + 1) No_frame in
									Array.blit full1 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									Array.blit full2 0 new_array (d - a) (e - d + 1);
									Range_full (a,e,new_array) :: tl
								)
								| Range_full (a,b,full1) :: Range_working c :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (b - a + 1 + num_frames) No_frame in
									Array.blit full1 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									Range_full (a,b_job,new_array) :: tl
								)
								| Range_working a :: Range_full (b,c,full2) :: tl when (a_job,b_job) = a -> (
									let new_array = Array.make (c - b + 1 + num_frames) No_frame in
									Array.blit temp_stats_array start_offset new_array 0 num_frames;
									Array.blit full2 0 new_array num_frames (c - b + 1);
									Range_full (a_job,c,new_array) :: tl
								)
								| Range_working a :: tl when (a_job,b_job) = a -> (
									Range_full (a_job,b_job, Array.sub temp_stats_array start_offset num_frames) :: tl
								)
								| hd :: tl -> hd :: rec_helper tl
								| [] -> ( (* Oops. *)
									print "ERROR: the working range (%d,%d) was not found! The stats list may have been hosed!" a_job b_job;
									[]
								)
							in
							print "full range done\n";
							rec_helper
						) else (
							let rec rec_helper = function (* Just part of it is done *)
								(*
									FWF
									WF
									FWE
									FWQ
									WE
									WQ
									FW
									W
								*)
								| Range_full (a,b,full1) :: Range_working c :: Range_full (d,e,full2) :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (last_i_frame_number - a + 1) No_frame in
									Array.blit full1 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									let new_full_range = Range_full (a,last_i_frame_number,new_array) in
              	
									(* Take a slice out of the second "full" range *)
									let first_i_frame = find_first_i_frame full2 0 in
									let first_i_frame_number = first_i_frame + d in
									if first_i_frame >= Array.length full2 - 1 then (
										print "WARNING: A finished range (%d,%d) was reduced to nothingness. This may or may not be bad, depending on how well I code the rest...\n" d e;
										match tl with
										| Range_empty (f,g) :: tl2 -> (
											(* Append to the next empty range *)
											new_full_range :: Range_empty (last_i_frame_number + 1, g) :: tl2
										)
										| _ -> (new_full_range :: Range_empty (last_i_frame_number + 1, e) :: tl)
									) else (
										(* Good. It's not toast. *)
										let new_second_full_range = Range_full (first_i_frame_number + 1, e, Array.sub full2 (first_i_frame + 1) (e - d + 1 - first_i_frame - 1)) in
										new_full_range :: Range_empty (last_i_frame_number + 1, first_i_frame_number) :: new_second_full_range :: tl
									)
								)
								| Range_working a :: Range_full (b,c,full) :: tl when (a_job,b_job) = a -> (
									let new_full_range = Range_full (a_job, last_i_frame_number, Array.sub temp_stats_array start_offset num_frames) in
              	
									(* Take a slice out of the second "full" range *)
									let first_i_frame = find_first_i_frame full 0 in
									let first_i_frame_number = first_i_frame + b in
									if first_i_frame >= Array.length full - 1 then (
										print "WARNING: A finished range (%d,%d) was reduced to nothingness. This may or may not be bad, depending on how well I code the rest...\n" b c;
										match tl with
										| Range_empty (d,e) :: tl2 -> (
											(* Append to the next empty range *)
											new_full_range :: Range_empty (last_i_frame_number + 1, e) :: tl2
										)
										| _ -> (new_full_range :: Range_empty (last_i_frame_number + 1, c) :: tl)
									) else (
										(* Good. It's not toast. *)
										let new_second_full_range = Range_full (first_i_frame_number + 1, c, Array.sub full (first_i_frame + 1) (c - b + 1 - first_i_frame - 1)) in
										new_full_range :: Range_empty (last_i_frame_number + 1, first_i_frame_number) :: new_second_full_range :: tl
									)
								)
								| Range_full (a,b,full) :: Range_working c :: Range_empty (d,e) :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (last_i_frame_number - a + 1) No_frame in
									Array.blit full 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									Range_full (a,last_i_frame_number,new_array) :: Range_empty (last_i_frame_number + 1, e) :: tl
								)
								| Range_full (a,b,full) :: Range_working c :: Range_queued (d,e) :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (last_i_frame_number - a + 1) No_frame in
									Array.blit full 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									Range_full (a,last_i_frame_number,new_array) :: Range_queued (last_i_frame_number + 1, e) :: tl
								)
								| Range_working a :: Range_empty (b,c) :: tl when (a_job,b_job) = a -> (
									let new_array = Array.sub temp_stats_array start_offset num_frames in
									Range_full (a_job,last_i_frame_number,new_array) :: Range_empty (last_i_frame_number + 1, c) :: tl
								)
								| Range_working a :: Range_queued (b,c) :: tl when (a_job,b_job) = a -> (
									let new_array = Array.sub temp_stats_array start_offset num_frames in
									Range_full (a_job,last_i_frame_number,new_array) :: Range_queued (last_i_frame_number + 1, c) :: tl
								)
								| Range_full (a,b,full) :: Range_working c :: tl when (a_job,b_job) = c -> (
									let new_array = Array.make (last_i_frame_number - a + 1) No_frame in
									Array.blit full 0 new_array 0 (b - a + 1);
									Array.blit temp_stats_array start_offset new_array (b - a + 1) num_frames;
									Range_full (a,last_i_frame_number,new_array) :: Range_empty (last_i_frame_number + 1, b_job) :: tl
								)
								| Range_working c :: tl when (a_job,b_job) = c -> (
									let new_array = Array.sub temp_stats_array start_offset num_frames in
									Range_full (a_job,last_i_frame_number,new_array) :: Range_empty (last_i_frame_number + 1, b_job) :: tl
								)
								| hd :: tl -> hd :: rec_helper tl
								| [] -> []
							in
							print "part range done\n";
							rec_helper
						)
					) in (* put_range_back_unsafe *)
					
					Mutex.lock range_list_mutex;
					print_log "finishing (%d,%d)\n" a_job b_job;
					range_list_ref := put_range_back_unsafe !range_list_ref;
print_ranges "finished up job\n";
					Mutex.unlock range_list_mutex;
				);
