open Types;;

let use_console_functions = try
	if Sys.os_type = "Win32" then (
		ignore (Console.get_console_text_attribute Console.std_output_handle);
		true
	) else (
		false
	)
with
	Console.Console_error ("get_console_text_attribute", 6) -> false
;;



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

let print = if use_console_functions then (
	(* With console functions *)
	fun ?(name="") ?(screen=true) ?(dump=true) ?(lock=true) ?(time=true) ?(fill=false) -> (
		let h f = (
			if screen || dump then (
				let time_string = if time then (time_string () ^ "  ") else "" in
				if lock then Mutex.lock pm;
				if screen then (
					Console.really_write Console.std_output_handle name;
					Console.really_write Console.std_output_handle f;
					if fill then ( (* Fill to the end of the line. DO NOT use a \n at the end of the string *)
						let info = Console.get_console_screen_buffer_info Console.std_output_handle in
						let num_spaces = info.Console.dwSizeX - info.Console.dwCursorX in
						Console.really_write Console.std_output_handle (String.make num_spaces ' ');
					)
				);
				if dump then (match !print_handle_ref with
					| None -> ()
					| Some print_out_handle -> (
						output_string print_out_handle time_string;
						output_string print_out_handle name;
						output_string print_out_handle f;
						if fill then (output_string print_out_handle "\n");
						flush print_out_handle
					)
				);
				if lock then Mutex.unlock pm;
			)
		) in
		fun a -> Printf.kprintf h a
	)
) else (
	(* Without console functions *)
	fun ?(name="") ?(screen=true) ?(dump=true) ?(lock=true) ?(time=true) ?(fill=false) -> (
		let h f = (
			if screen || dump then (
				let time_string = if time then (time_string () ^ "  ") else "" in
				if lock then Mutex.lock pm;
				if screen then (Printf.printf "%s%s%s" name f (if fill then "\n" else ""); flush stdout);
				if dump then (match !print_handle_ref with
					| None -> ()
					| Some print_out_handle -> (
						output_string print_out_handle time_string;
						output_string print_out_handle name;
						output_string print_out_handle f;
						if fill then (output_string print_out_handle "\n");
						flush print_out_handle
					)
				);
				if lock then Mutex.unlock pm;
			)
		) in
		fun a -> Printf.kprintf h a
	)
);;

(***********************************)
(* A few controller-specific types *)
(***********************************)
type agent_status_t =
	| S_Disconnected
	| S_Connected of (string * int)
	| S_Waiting
	| S_Checked_out of (int * int)
	| S_Sending of (int * int)
	| S_Doing of (int * int)
	| S_Dead
	| S_Done of (int * int)
	| S_Failed of (int * int)
	| S_Exited
;;

type agent1_thread_t = {
	mutable a1_current_job : (int * int) option;
	mutable a1_status : agent_status_t;
	mutable a1_total_frames : int;
	mutable a1_total_time : float; (* in seconds *)
	mutable a1_last_start : float;
	a1_from_config : bool;
	a1_thread : Thread.t;
};;

type agent2_thread_t = {
	mutable a2_current_job : transit_gop_t option;
	mutable a2_status : agent_status_t;
	mutable a2_total_frames : int;
	mutable a2_total_time : float;
	mutable a2_last_start : float;
	a2_from_config : bool;
	mutable a2_kill_ok : bool;
	a2_thread : Thread.t;
};;

let status_splitter stat =
	let s a = List.sort compare a in
	let rec ss (dis,con,wai,che,sen,doi,dea,don,fai,exi) = function
		| [] -> (dis, s con, wai, s che, s sen, s doi, dea, s don, s fai, exi)
		| S_Disconnected  :: x -> ss ( dis+1  ,con,wai,che,sen,doi,dea,don,fai,exi) x
		| S_Connected a   :: x -> ss (dis, a::con ,wai,che,sen,doi,dea,don,fai,exi) x
		| S_Waiting       :: x -> ss (dis,con, wai+1  ,che,sen,doi,dea,don,fai,exi) x
		| S_Checked_out a :: x -> ss (dis,con,wai, a::che ,sen,doi,dea,don,fai,exi) x
		| S_Sending a     :: x -> ss (dis,con,wai,che, a::sen ,doi,dea,don,fai,exi) x
		| S_Doing a       :: x -> ss (dis,con,wai,che,sen, a::doi ,dea,don,fai,exi) x
		| S_Dead          :: x -> ss (dis,con,wai,che,sen,doi, dea+1  ,don,fai,exi) x
		| S_Done a        :: x -> ss (dis,con,wai,che,sen,doi,dea, a::don ,fai,exi) x
		| S_Failed a      :: x -> ss (dis,con,wai,che,sen,doi,dea,don, a::fai ,exi) x
		| S_Exited        :: x -> ss (dis,con,wai,che,sen,doi,dea,don,fai, exi+1  ) x
	in
	ss (0,[],0,[],[],[],0,[],[],0) stat
;;

