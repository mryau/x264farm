
let print_handle_ref : out_channel option ref = ref None;;



(***************)
(* Print mutex *)
(***************)
let time_string () = (
	let tod = Unix.gettimeofday () in
	let cs = int_of_float (100.0 *. (tod -. floor tod)) in (* centi-seconds *)
	let lt = Unix.localtime tod in
	Printf.sprintf "%04d-%02d-%02d~%02d:%02d:%02d.%02d" (lt.Unix.tm_year + 1900) (lt.Unix.tm_mon + 1) (lt.Unix.tm_mday) (lt.Unix.tm_hour) (lt.Unix.tm_min) (lt.Unix.tm_sec) cs
);;
(*let print_out_handle = open_out "out-dump.txt";;*)
let pm = Mutex.create ();;

let print = fun ?(name="") ?(screen=true) ?(dump=true) ?(lock=true) ?(time=true) (*?(fill=false)*) -> (
	let h f = (
		if screen || dump then (
			let time_string = if time then (time_string () ^ "  ") else "" in
			if lock then Mutex.lock pm;
(*			if screen then (Printf.printf "%s%s%s" name f (if fill then "\n" else ""); flush stdout);*)
			if screen then (Printf.printf "%s%s" name f; flush stdout);
			if dump then (match !print_handle_ref with
				| None -> ()
				| Some print_out_handle -> (
					output_string print_out_handle time_string;
					output_string print_out_handle name;
					output_string print_out_handle f;
(*					if fill then (output_string print_out_handle "\n");*)
					flush print_out_handle
				)
			);
			if lock then Mutex.unlock pm;
		)
	) in
	fun a -> Printf.kprintf h a
);;

