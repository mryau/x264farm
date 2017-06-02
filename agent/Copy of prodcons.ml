type 'a prodcons =
  { buffer: 'a array;
    lock: Mutex.t;
    mutable readpos: int;
    mutable writepos: int;
    notempty: Condition.t;
    notfull: Condition.t }

let create size init =
  { buffer = Array.make size init;
    lock = Mutex.create ();
    readpos = 0;
    writepos = 0;
    notempty = Condition.create();
    notfull = Condition.create() }

let put p data =
  Mutex.lock p.lock;
  while (p.writepos + 1) mod Array.length p.buffer = p.readpos do
(*  	Printf.printf "Buffer full\n"; flush stdout;*)
    Condition.wait p.notfull p.lock
  done;
  p.buffer.(p.writepos) <- data;
  p.writepos <- (p.writepos + 1) mod Array.length p.buffer;
  Condition.signal p.notempty;
  Mutex.unlock p.lock

let get p =
  Mutex.lock p.lock;
  while p.writepos = p.readpos do
(*  	Printf.printf "Buffer empty\n"; flush stdout;*)
    Condition.wait p.notempty p.lock
  done;
  let data = p.buffer.(p.readpos) in
  p.readpos <- (p.readpos + 1) mod Array.length p.buffer;
  Condition.signal p.notfull;
  Mutex.unlock p.lock;
  data







type string_prodcons = {
	s_buffer : string array;
	s_valid_bytes : int array;
	s_length : int;
	s_lock : Mutex.t;
	mutable s_readpos : int;
	mutable s_writepos : int;
	s_notempty : Condition.t;
	s_notfull : Condition.t;
};;


(*

let s_create size init_bytes =
	{
		s_buffer = Array.init (fun _ -> String.create init_bytes) size;
		s_valid_bytes = Array.make size 0;
		s_lock = Mutex.create ();
		s_readpos = 0;
		s_writepos = 0;
		s_notempty = Condition.create ();
		s_notfull = Condition.create ();
	}
;;

let s_put p data =
	Mutex.lock p.s_lock;
  while (p.s_writepos + 1) mod Array.length p.s_buffer = p.s_readpos do
    Condition.wait p.s_notfull p.s_lock
  done;
  p.s_buffer.(p.writepos) <- data;

	if String.length data > String.length p.s_buffer.(p.s_writepos) then (
		(* Need to ... *)

  p.s_writepos <- (p.s_writepos + 1) mod Array.length p.s_buffer;
  Condition.signal p.s_notempty;
	Mutex.unlock p.s_lock;
*)

(* Test *)
(*
let buff = create 20 0

let rec produce n =
  print_int n; print_string "-->"; print_newline();
  put buff n;
  produce (n+1)

let rec consume () =
  let n = get buff in
  print_string "-->"; print_int n; print_newline();
  consume ()

let _ =
  Thread.create produce 0;
  consume()
*)
