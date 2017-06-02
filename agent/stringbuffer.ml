(* ocamlopt -o stringbuffer -thread unix.cmxa threads.cmxa stringbuffer.ml ; ./stringbuffer *)

type buf_t = {
	buf : string;
	mutable input : int;  (* Start writing here *)
	mutable output : int; (* Start reading here *)
	mutable last : bool;
	mutex : Mutex.t;
	empty : Condition.t; (* Bad for reader *)
	full : Condition.t;  (* Bad for writer *)
};;

(*
 * Valid data goes from OUT to IN-1
 * If out = in:
	 * Write up to (len - 1)
	 * Read BLOCKS
 * If in is 1 LESS than out OR if out is 0 and in is (len - 1):
	 * Write BLOCKS
	 * Read up to (len - 1)
 * If in < out:
	 * Write up to out - in - 1 bytes; in <- out - 1
	 * Read up to end; in <- len (=0)
 * If out < in:
	 * Read up to out - in bytes; out <- in
	 * Write up to end; in <- len (=0)
 *)

let create len = (
	{
		buf = String.create len;
		input = 0;
		output = 0;
		last = false;
		mutex = Mutex.create ();
		empty = Condition.create ();
		full = Condition.create ();
	}
);;

let pm = Mutex.create ();;
let printf = (
	let h f = (
(*
		Mutex.lock pm;
		Printf.printf "%s" f;
		Mutex.unlock pm;
*)
	) in
	fun a -> Printf.kprintf h a
);;

(*
let rec write_unsafe b s o l so_far = (
	printf "  Write: %d bytes, starting at %d, can't overwrite %d\n" l b.input b.output;
	if l = 0 then (
		printf "    Write: %d bytes total %d,%d\n" so_far b.input b.output;
		if so_far > 0 then (
			printf "    Write: signalling EMPTY\n";
			Condition.signal b.empty;
		)
	) else if (
		(b.input = String.length b.buf - 1 && b.output = 0) ||
		(b.input = b.output - 1)
	) then (
		(* There's no room! *)
		printf "    Write: no room; waiting on FULL %d,%d\n" b.input b.output;
		Condition.wait b.full b.mutex;
		write_unsafe b s o l so_far
	) else if b.input >= b.output then (
		(* Can write to the end of the buffer *)
		if b.output = 0 then (
			printf "    Write: writing to ALMOST the end of the buffer %d,%d\n" b.input b.output;
			let write_len = min l (String.length b.buf - b.input - 1) in
			String.blit s o b.buf b.input write_len;
			b.input <- (b.input + write_len) mod (String.length b.buf);
			write_unsafe b s (o + write_len) (l - write_len) (so_far + write_len)
		) else (
			printf "    Write: writing to end of buffer %d,%d\n" b.input b.output;
			let write_len = min l (String.length b.buf - b.input) in
			String.blit s o b.buf b.input write_len;
			b.input <- (b.input + write_len) mod (String.length b.buf);
			write_unsafe b s (o + write_len) (l - write_len) (so_far + write_len)
		)
	) else (
		(* b.input < b.output *)
		printf "    Write: normally %d,%d\n" b.input b.output;
		let write_len = min l (b.output - b.input - 1) in
		String.blit s o b.buf b.input write_len;
		b.input <- b.input + write_len; (* No need to mod, since we didn't overflow *)
		write_unsafe b s (o + write_len) (l - write_len) (so_far + write_len)
	)
);;
*)

let rec write_unsafe b s o l = (
	printf "  Write: %d bytes, starting at %d, can't overwrite %d\n%!" l b.input b.output;
	if l = 0 then (
		printf "    Write: nothing more to write %d,%d\n%!" b.output b.input;
	) else (
		let writable_len = (b.output - b.input - 1 + String.length b.buf) mod (String.length b.buf) in
		printf "    Write: there are %d writable bytes %d,%d\n%!" writable_len b.output b.input;
		if writable_len = 0 then (
			(* l > 0, but no place to put it *)
			printf "    Write: Oops. That's zero, isn't it... wait on FULL %d,%d\n%!" b.output b.input;
			Condition.wait b.full b.mutex;
			printf "    Write: trying again... %d,%d\n%!" b.output b.input;
			write_unsafe b s o l ;
		) else (
			let write_len = min (min l writable_len) (String.length b.buf - b.input) in
			printf "    Write: Can write %d bytes now %d,%d\n%!" write_len b.output b.input;
			String.blit s o b.buf b.input write_len;
			b.input <- (b.input + write_len) mod (String.length b.buf);
			(* Must do mutex here since something has been written and the read may be waiting on it *)
			Condition.signal b.empty;
			write_unsafe b s (o + write_len) (l - write_len)
		)
	)
);;

let write b s o l = (
	if l > String.length s - o || o < 0 then raise (Invalid_argument "Stringbuffer.write");
	if b.last && l <> 0 then raise End_of_file; (* Don't write if last is set *)
	Mutex.lock b.mutex;
	write_unsafe b s o l;
	Mutex.unlock b.mutex;
);;

let write_string b s = (
	write b s 0 (String.length s)
);;

let rec read_unsafe b s o l so_far = (
	printf "  Read: %d bytes, starting at %d, can't overread %d\n%!" l b.output b.input;
	if b.output = b.input then (
		printf "    Read: Nothing to read %d,%d\n%!" b.output b.input;
		if b.last then (
			printf "    Read: LAST! Returning 0\n%!";
			0
		) else (
			(* Block on b.empty *)
			printf "    Read: waiting on EMPTY\n%!";
			Condition.wait b.empty b.mutex;
			printf "    Read: finally going?\n%!";
			read_unsafe b s o l so_far
		)
	) else (
		let readable_len = (b.input - b.output + String.length b.buf) mod (String.length b.buf) in
		let read_len = min (min l readable_len) (String.length b.buf - b.output) in (* I think I did the write length wrong... *)
		printf "    Read: There are %d valid bytes; read %d of them %d,%d\n%!" readable_len read_len b.output b.input;
		String.blit b.buf b.output s o read_len;
		b.output <- (b.output + read_len) mod (String.length b.buf);
		read_len
	)
);;

let read b s o l = (
	if l > String.length s - o || o < 0 then raise (Invalid_argument "Stringbuffer.read");
	Mutex.lock b.mutex;
	let q = read_unsafe b s o l 0 in
	if q > 0 then Condition.signal b.full;
	Mutex.unlock b.mutex;
	q
);;

let really_read b s o l = (
	if l > String.length s - o || o < 0 then raise (Invalid_argument "Stringbuffer.really_read");
	let read_internal b s o l = (
		Mutex.lock b.mutex;
		let q = read_unsafe b s o l 0 in
		if q > 0 then Condition.signal b.full;
		Mutex.unlock b.mutex;
		q
	) in
	let rec help o2 l2 = if l2 = 0 then () else (
		let got = read_internal b s o2 l2 in
		if got = 0 then (
			(* The only time it will return 0 is if that's what was requested or last has been set *)
			raise End_of_file
		) else (
			help (o2 + got) (l2 - got)
		)
	) in
	help o l
);;

let close b = (
	Mutex.lock b.mutex;
	b.last <- true;
	Condition.signal b.empty; (* Let the readers restart so they can see the last flag *)
	Mutex.unlock b.mutex;
);;

let reset b = (
	Mutex.lock b.mutex;
	b.last <- false;
	b.input <- 0;
	b.output <- 0;
	Condition.signal b.full; (* The buffer is now empty; restart the writers *)
	Mutex.unlock b.mutex;
);;

let length b = (
	Mutex.lock b.mutex;
	let q = (b.input - b.output + String.length b.buf - b.output) in
	Mutex.unlock b.mutex;
	q
);;

(************)
(* DO STUFF *)
(************)
(*

if false then (
	let print_buf b = (
		printf "Valid from %3d to %3d, last %B\n%!" b.output b.input b.last;
		printf "Contents: %S\n%!" b.buf;
	) in

	let b = create 8 in

	print_buf b;
	write b "1111" 0 4;

	print_buf b;
	write b "222#" 0 3;

	print_buf b;
	write b "" 0 0;

	let temp = String.make 8 '-' in
	print_buf b;
	printf "Got %d bytes\n%!" (read b temp 0 2);
	printf "  %S\n" temp;

	print_buf b;
	write b "$$" 0 2;

	print_buf b;
	printf "Got %d bytes\n%!" (read b temp 0 8);
	really_read b temp 0 1;
	printf "  %S\n" temp;

	print_buf b;
	write b "%%%" 0 3;

	print_buf b;
	printf "Got %d bytes\n%!" (read b temp 0 4);
	printf "  %S\n" temp;

	print_buf b
);;

let b = create 8;;

let read_data_ref = ref 0;;
let read_data_mutex = Mutex.create ();;
let read_data_condition = Condition.create ();;

let read_thread () = (
	while true do
		Mutex.lock read_data_mutex;
		while !read_data_ref = 0 do
			printf "Reader waiting...\n%!";
			Condition.wait read_data_condition read_data_mutex;
		done;
		let len = !read_data_ref in
		read_data_ref := 0;
		let temp = String.make len '~' in
		let num = read b temp 0 len in
		printf "  RRRR: %S (%d)\n%!" temp num;
		Mutex.unlock read_data_mutex;
	done
);;

let r_thread = Thread.create read_thread ();;


let write_data_ref = ref "";;
let write_data_mutex = Mutex.create ();;
let write_data_condition = Condition.create ();;

let write_thread () = (
	while true do
		Mutex.lock write_data_mutex;
		while !write_data_ref = "" do
			printf "Writer waiting...\n%!";
			Condition.wait write_data_condition write_data_mutex;
		done;
		let d = !write_data_ref in
		write_data_ref := "";
		write b d 0 (String.length d);
		printf "  WWWW: %d\n%!" (String.length d);
		Mutex.unlock write_data_mutex;
	done
);;

let w_thread = Thread.create write_thread ();;






while true do
	try
		flush stdout;
		let a = read_line () in
		let (f,n) = Scanf.sscanf a "%c %s" (fun a b -> (a,b)) in
		printf "Passed %c %s\n%!" f n;
		match f with
		| 'r' -> (
			Mutex.lock read_data_mutex;
			read_data_ref := (int_of_string n);
			Condition.signal read_data_condition;
			Mutex.unlock read_data_mutex;
		)
		| 'w' -> (
			Mutex.lock write_data_mutex;
			write_data_ref := n;
			Condition.signal write_data_condition;
			Mutex.unlock write_data_mutex;
		)
		| 'z' -> (
			Mutex.lock write_data_mutex;
			close b;
			Mutex.unlock write_data_mutex;
		)
		| 'a' -> (
			Mutex.lock write_data_mutex;
			reset b;
			Mutex.unlock write_data_mutex;
		)
		| _ -> (printf "???\n%!")
	with
	| Scanf.Scan_failure _ | Failure "int_of_string" -> printf "???\n%!"
done;;
*)

