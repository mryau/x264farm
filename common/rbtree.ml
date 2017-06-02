type color_t = Red | Black;;

type ('a,'b) node_t = {
	mutable parent : ('a,'b) node_t option;
	mutable color : color_t;
	mutable key : 'a;
	mutable contents : 'b;
	mutable left : ('a,'b) node_t option;
	mutable right : ('a,'b) node_t option;
};;

type ('a,'b) t = {
	cmp : 'a -> 'a -> int;
	mutable nodes : int;
	mutable root : ('a,'b) node_t option;
};;

let grandparent = function
	| {parent = Some {parent = Some x}} -> Some x
	| _ -> None
;;

let uncle = function
	| {parent = Some ({parent = Some {left = Some q; right = Some r}} as p)} when p == q -> Some r (* Parent is the LEFT node of the grandparent *)
	| {parent = Some ({parent = Some {left = Some q; right = Some r}} as p)} when p == r -> Some r (* Parent is the RIGHT node of the grandparent *)
	| _ -> None
;;

let sibling = function
	| {parent = Some {left = Some pl; right = pr}} as n when n == pl -> pr
	| {parent = Some {left = pl}}                                    -> pl
	| _ -> None
;;

let sibling_color n = match sibling n with
	| Some x -> x.color
	| None -> Black (* All "None" node placeholders are black *)
;;

(* (parent, uncle, grandparent) *)
let pug = function
	| {parent = Some ({parent = Some ({left = Some q; right = Some r} as g)} as p)} when p == q -> (Some p, Some r, Some g) (* Parent is the LEFT node of the grandparent *)
	| {parent = Some ({parent = Some ({left = Some q; right = Some r} as g)} as p)}             -> (Some p, Some q, Some g) (* Parent is the RIGHT node of the grandparent *)
	| {parent = None} -> (None, None, None)
	| {parent = Some ({parent = None} as p)} -> (Some p, None, None)
	| {parent = Some ({parent = Some (({left = None} | {right = None}) as g)} as p)} -> (Some p, None, Some g)
;;

let pugs n = (
	let (p,u,g) = pug n in
	let s = sibling n in
	(p,u,g,s)
);;

let create ?(cmp=compare) () = {cmp = cmp; nodes = 0; root = None};;

(************)
(* PRINTING *)
(************)
let more_tabs = "  ";;
let rec print_rec n tabs print_key print_val = (
	Printf.printf "%s%s%s = %s\n" tabs (match n.color with Red -> "r " | Black -> "b ") (print_key n.key) (print_val n.contents);
	(match n.left with
		| Some x -> print_rec x (tabs ^ more_tabs) print_key print_val
		| None -> ()
	);
	(match n.right with
		| Some x -> print_rec x (tabs ^ more_tabs) print_key print_val
		| None -> ()
	);
);;
let print q print_key print_val = (
	Printf.printf "TREE: %d\n" q.nodes;
	match q.root with
	| None -> Printf.printf " No nodes!\n";
	| Some n -> print_rec n more_tabs print_key print_val
);;

let rec print_ordered_rec n tabs print_key print_val = (
	let parent_string = (match n.parent with
		| None -> "no parent"
		| Some x -> (print_key x.key)
	) in
	(match n.right with
		| Some x -> print_ordered_rec x (tabs ^ more_tabs) print_key print_val
		| None -> ()
	);
	Printf.printf "%s%s%s = %s (%s)\n" tabs (match n.color with Red -> "r " | Black -> "b ") (print_key n.key) (print_val n.contents) parent_string;
	(match n.left with
		| Some x -> print_ordered_rec x (tabs ^ more_tabs) print_key print_val
		| None -> ()
	);
);;
let print_ordered q print_key print_val = (
	Printf.printf "TREE: %d\n" q.nodes;
	match q.root with
	| None -> Printf.printf " No nodes!\n";
	| Some n -> print_ordered_rec n more_tabs print_key print_val
);;
(*
let rec pp2 = function
	| {parent = Some p} -> pp2 p
	| a -> print_ordered_rec a more_tabs string_of_int string_of_int
;;
*)
let rec pp2 x = ();;
let pp x = ();;
let print_this x = ();; (* print_string x *)

(*************)
(* INSERTION *)
(*************)
let rec insert_case_1 t = function
		(* "I see a red root and I want it painted black" *)
	| {parent = None} as n -> (print_this "1.1\n"; pp n; n.color <- Black)
	| n -> (print_this "1.2\n"; pp n; insert_case_2 t n)
and insert_case_2 t = function
	| {parent = Some {color = Black}} -> (print_this "2.1\n") (* OK! *)
	| n -> (print_this "2.2\n"; pp n; insert_case_3 t n)
and insert_case_3 t n = match pug n with
	| (Some p, Some ({color = Red} as u), Some g) -> (print_this "3.1\n"; pp n; p.color <- Black; u.color <- Black; g.color <- Red; insert_case_1 t g)
	| _ -> (print_this "3.2\n"; pp n; insert_case_4 t n)
and insert_case_4 t n = match pug n with
	| (Some ({right = Some pr} as p), _, Some ({left = Some gl} as g)) when n == pr && p == gl -> (
		print_this "4.1\n"; pp n;
		g.left <- Some n;
		let n_old_left = n.left in
		n.left <- Some p;
		n.parent <- Some g;
		p.right <- n_old_left;
		(match p.right with
			| None -> ()
			| Some x -> x.parent <- Some p
		);
		p.parent <- Some n;
		insert_case_5 t p
	)
	| (Some ({left = Some pl} as p), _, Some ({right = Some gr} as g)) when n == pl && p == gr -> (
		print_this "4.2\n"; pp n;
		g.right <- Some n;
		let n_old_right = n.right in
		n.right <- Some p;
		n.parent <- Some g;
		p.left <- n_old_right;
		(match p.left with
			| None -> ()
			| Some x -> x.parent <- Some p
		);
		p.parent <- Some n;
		insert_case_5 t p
	)
	| _ -> (print_this "4.3\n"; pp n; insert_case_5 t n)
and insert_case_5 t n = match pug n with
	| (Some ({left = Some pl} as p), _, Some ({left = Some gl} as g)) when n == pl && p == gl -> (
		print_this "5.1\n"; pp n;
		p.color <- Black;
		g.color <- Red;
		let g_old_parent = g.parent in
		g.left <- p.right;
		(match g.left with (* Fix the grandparent's new left kid to point to g as its parent *)
			| None -> ()
			| Some x -> x.parent <- Some g
		);
		g.parent <- Some p;
		p.right <- Some g;
		p.parent <- g_old_parent;

		(* Fix the great-grandparent to point to the old parent *)
		match p.parent with
		| None -> t.root <- Some p
		| Some ({left = Some ggl} as gg) when ggl == g -> (gg.left <- Some p)
		| Some gg -> (gg.right <- Some p)
	)
	| (Some ({right = Some pr} as p), _, Some ({right = Some gr} as g)) when n == pr && p == gr -> (
		print_this "5.2\n"; pp2 n;

(*		Printf.printf "N: %d; P: %d; G: %d\n" n.key p.key g.key;*)

		p.color <- Black;
		g.color <- Red;
		let g_old_parent = g.parent in
		g.right <- p.left;
		(match g.right with (* Fix the grandparent's new right kid to point to g as its parent *)
			| None -> ()
			| Some x -> x.parent <- Some g
		);
		g.parent <- Some p;
		p.left <- Some g;
		p.parent <- g_old_parent;
		print_this "5.2.7\n"; pp2 p;

		(* Fix the great-grandparent to point to the old parent *)
		(match g_old_parent with
			| None -> t.root <- Some p
			| Some ({left = Some ggl} as gg) when ggl == g -> (gg.left <- Some p)
			| Some gg -> (gg.right <- Some p)
		);
	)
	| _ -> (print_this "5.3\n"; pp n) (* Done? *)
;;

let rec insert_rec t q a b = (
	let c = t.cmp a q.key in
	if c = 0 then (
		(* Replace! *)
		q.key <- a;
		q.contents <- b;
	) else if c < 0 then ( (* a < q.key *)
		match q.left with
		| None -> (
			let n = {parent = Some q; color = Red; key = a; contents = b; left = None; right = None} in
			q.left <- Some n;
			t.nodes <- succ t.nodes;
			insert_case_1 t n
		)
		| Some l -> insert_rec t l a b
	) else ( (* a > q.key *)
		match q.right with
		| None -> (
			let n = {parent = Some q; color = Red; key = a; contents = b; left = None; right = None} in
			q.right <- Some n;
			t.nodes <- succ t.nodes;
			insert_case_1 t n
		)
		| Some r -> insert_rec t r a b
	)
);;

let rec add t a b = (
	match t.root with
	| None -> (t.nodes <- succ t.nodes; t.root <- Some {parent = None; color = Black; key = a; contents = b; left = None; right = None})
	| Some q -> insert_rec t q a b
);;

(*************)
(* SEARCHING *)
(*************)

let rec find_node_guts t key node = (
	let c = t.cmp key node.key in
	match (node.left, node.right, c < 0, c > 0) with
	| (None  , _, true, _) -> None
	| (Some x, _, true, _) -> find_node_guts t key x
	| (_, None  , _, true) -> None
	| (_, Some x, _, true) -> find_node_guts t key x
	| (_, _, false, false) -> Some node
);;
let find_node t key =
	match t.root with
	| None -> None
	| Some x -> find_node_guts t key x
;;

let find t key =
	match find_node t key with
	| None -> None
	| Some x -> Some (x.key, x.contents)
;;

(**********************)
(* OTHER SEARCH STUFF *)
(**********************)
let rec first_node_guts = function
	| {left = Some x} -> first_node_guts x
	| n -> n
;;
let rec last_node_guts = function
	| {right = Some x} -> last_node_guts x
	| n -> n
;;

let first_node = function
	| {root = Some x} -> Some (first_node_guts x)
	| _ -> None
;;
let last_node = function
	| {root = Some x} -> Some (last_node_guts x)
	| _ -> None
;;

let first t = match first_node t with
	| Some x -> Some (x.key, x.contents)
	| None -> None
;;
let last t = match last_node t with
	| Some x -> Some (x.key, x.contents)
	| None -> None
;;

(* These two functions return the node just to the right of the selected node *)
let rec go_up_and_left = function
	| {parent = Some ({right = Some pr} as p)} as n when n == pr -> go_up_and_left p (* Parent's on the left side *)
	| {parent = x} -> x (* Parent's either on the right side (return it) or doesn't exist (return None) *)
;;
let next_node_from = function
	| {right = Some r} -> Some (first_node_guts r)
	| {parent = None} -> None (* It's the root, and there's nothing to the right of it *)
	| {parent = Some p} as n -> go_up_and_left n
;;

(* These two functions return the node just to the right of the selected node *)
let rec go_up_and_right = function
	| {parent = Some ({left = Some pl} as p)} as n when n == pl -> go_up_and_right p (* Parent's on the left side *)
	| {parent = x} -> x (* Parent's either on the right side (return it) or doesn't exist (return None) *)
;;
let prev_node_from = function
	| {left = Some l} -> Some (last_node_guts l)
	| {parent = None} -> None (* It's the root, and there's nothing to the right of it *)
	| {parent = Some p} as n -> go_up_and_right n
;;

let rec find_near_guts t key node = (
	let c = t.cmp key node.key in
	match (node.left, node.right, c < 0, c > 0) with
	| (None  , _, true, _) -> Some node
	| (Some x, _, true, _) -> find_near_guts t key x
	| (_, None  , _, true) -> Some node
	| (_, Some x, _, true) -> find_near_guts t key x
	| (_, _, false, false) -> Some node
);;
let find_node_near t key =
	match t.root with
	| None -> None
	| Some x -> find_near_guts t key x
;;

let find_smallest_greater_than t key = (
	match find_node_near t key with
	| None -> None (* Oops. No keys *)
	| Some x when t.cmp key x.key < 0 -> Some (x.key, x.contents)
	| Some x -> (match next_node_from x with
		| None -> None
		| Some y -> Some (y.key, y.contents)
	)
);;

let find_largest_less_than t key = (
	match find_node_near t key with
	| None -> None (* Oops. No keys *)
	| Some x when t.cmp key x.key > 0 -> Some (x.key, x.contents)
	| Some x -> (match prev_node_from x with
		| None -> None
		| Some y -> Some (y.key, y.contents)
	)
);;

let find_smallest_not_less_than t key = (
	match find_node_near t key with
	| None -> None (* Oops. No keys *)
	| Some x when t.cmp key x.key > 0 -> (match next_node_from x with
		| None -> None
		| Some y -> Some (y.key, y.contents)
	)
	| Some x -> Some (x.key, x.contents)
);;

let find_largest_not_greater_than t key = (
	match find_node_near t key with
	| None -> None (* Oops. No keys *)
	| Some x when t.cmp key x.key < 0 -> (match prev_node_from x with
		| None -> None
		| Some y -> Some (y.key, y.contents)
	)
	| Some x -> Some (x.key, x.contents)
);;


(********)
(* ITER *)
(********)
let rec iter_guts func node = (
	(match node.left with
		| None -> ()
		| Some x -> iter_guts func x
	);
	let () = func node.key node.contents in
	(match node.right with
		| None -> ()
		| Some x -> iter_guts func x
	);
);;
let iter func = function
	| {root = Some x} -> iter_guts func x
	| x -> ()
;;

(********)
(* FOLD *)
(********)
let rec fold_left_guts func so_far n =
	let new_so_far = func so_far n.key n.contents in
	match next_node_from n with
	| None -> new_so_far
	| Some n2 -> fold_left_guts func new_so_far n2
;;
let fold_left func so_far t =
	match t.root with
	| None -> so_far
	| Some x -> fold_left_guts func so_far (first_node_guts x)
;;

let rec fold_right_guts func so_far n =
	let new_so_far = func so_far n.key n.contents in
	match prev_node_from n with
	| None -> new_so_far
	| Some n2 -> fold_right_guts func new_so_far n2
;;
let fold_right func so_far t =
	match t.root with
	| None -> so_far
	| Some x -> fold_right_guts func so_far (last_node_guts x)
;;


(************)
(* DELETION *)
(************)

let rec delete_case_1 t = function
	| {parent = None} as n -> (print_this "1.1\n"; pp2 n) (* Done! *)
	| n -> (print_this "1.2\n"; pp2 n; delete_case_2 t n)
and delete_case_2 t n = match pugs n with
	| (Some p,_,g_perhaps,Some ({color = Red} as s)) -> (
		print_this "2.1\n"; pp2 n;
		p.color <- Red;
		s.color <- Black;
		(match p.left with
			| Some pl when pl == n -> (
				print_this "2.1.1\n"; pp2 n;
				p.parent <- Some s;
				let old_sl = s.left in
				s.left <- Some p;
				p.right <- old_sl;
				(match p.right with
					| None -> ()
					| Some new_pr -> new_pr.parent <- Some p
				);
				s.parent <- g_perhaps;
				(match g_perhaps with
					| None -> (t.root <- Some s) (* The parent is the root! *)
					| Some ({left = Some gl} as g) when gl == p -> g.left <- Some s
					| Some g -> g.right <- Some s
				)
			)
			| _ -> (
				print_this "2.1.2\n"; pp2 n;
				p.parent <- Some s;
				let old_sr = s.right in
				s.right <- Some p;
				p.left <- old_sr;
				(match p.left with
					| None -> ()
					| Some new_pl -> new_pl.parent <- Some p
				);
				s.parent <- g_perhaps;
				(match g_perhaps with
					| None -> (t.root <- Some s) (* The parent is the root! *)
					| Some ({left = Some gl} as g) when gl == p -> g.left <- Some s
					| Some g -> g.right <- Some s
				)
			)
		);
		print_this "2.1 (end)\n"; pp2 n;
		delete_case_3 t n
	)
	| _ -> (print_this "2.2\n"; pp2 n; delete_case_3 t n)
and delete_case_3 t n = match pugs n with
	| (Some ({color = Black} as p), _, _, Some ({color = Black; left = (None | Some {color = Black}); right = (None | Some {color = Black})} as s)) -> (print_this "3.1\n"; pp2 n; s.color <- Red; delete_case_1 t p)
	| _ -> (print_this "3.2\n"; pp2 n; delete_case_4 t n)
and delete_case_4 t n = match pugs n with
	| (Some ({color = Red} as p), _, _, Some ({color = Black; left = (None | Some {color = Black}); right = (None | Some {color = Black})} as s)) -> (print_this "4.1\n"; pp2 n; p.color <- Black; s.color <- Red)
	| _ -> (print_this "4.2\n"; pp2 n; delete_case_5 t n)
and delete_case_5 t n = match pugs n with
	| (Some ({left = Some pl} as p), _, _, Some ({color = Black; left = Some ({color = Red} as sl); right = (None | Some {color = Black})} as s)) when pl == n -> (
		print_this "5.1\n"; pp2 n;
		s.color <- Red;
		sl.color <- Black;
		s.left <- sl.right;
		(match s.left with
			| Some slr -> slr.parent <- Some s
			| _ -> ()
		);
		s.parent <- Some sl;
		sl.right <- Some s;
		sl.parent <- Some p;
		p.right <- Some sl;
		delete_case_6 t n
	)
	| (Some ({right = Some pr} as p), _, _, Some ({color = Black; right = Some ({color = Red} as sr); left = (None | Some {color = Black})} as s)) when pr == n -> (
		print_this "5.2\n"; pp2 n;
		s.color <- Red;
		sr.color <- Black;
		s.right <- sr.left;
		(match s.right with
			| Some srl -> srl.parent <- Some s
			| _ -> ()
		);
		s.parent <- Some sr;
		sr.left <- Some s;
		sr.parent <- Some p;
		p.left <- Some sr;
		delete_case_6 t n
	)
	| _ -> (print_this "5.3\n"; pp2 n; delete_case_6 t n)
and delete_case_6 t n = match pugs n with
	| (Some ({left = Some pl} as p), _, _, Some ({color = Black; right = Some ({color = Red} as sr)} as s)) when n == pl -> (
		print_this "6.1\n"; pp2 n;
		s.color <- p.color;
		p.color <- Black;
		sr.color <- Black;
		p.right <- s.left;
		(match p.right with
			| Some x -> x.parent <- Some p
			| None -> ()
		);
		s.parent <- p.parent;
		(match s.parent with
			| Some ({left = Some gl} as g) when gl == p -> g.left <- Some s
			| Some g -> g.right <- Some s
			| _ -> (t.root <- Some s) (* The parent is the root! *)
		);
		p.parent <- Some s;
		s.left <- Some p;
	)
	| (Some ({right = Some pr} as p), _, _, Some ({color = Black; left = Some ({color = Red} as sl)} as s)) when n == pr -> (
		print_this "6.2\n"; pp2 n;
		s.color <- p.color;
		p.color <- Black;
		sl.color <- Black;
		p.left <- s.right;
		(match p.left with
			| Some x -> x.parent <- Some p
			| None -> ()
		);
		s.parent <- p.parent;
		(match s.parent with
			| Some ({right = Some gr} as g) when gr == p -> g.right <- Some s
			| Some g -> g.left <- Some s
			| _ -> (t.root <- Some s) (* The parent is the root! *)
		);
		p.parent <- Some s;
		s.right <- Some p;
	)
	| _ -> (print_this "6.3?!\n"; pp2 n) (* Uhh... it's done, although I don't know if it can get here... *)
;;

let remove_leaf t = function
	| {parent = None} -> (failwith "tried to remove leaf on the root") (* WHAT! This is the root? WRONG! *)
	(* Red nodes can be deleted with no problems *)
	| {color = Red; left  = Some l; parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some l; l.parent <- Some p)
	| {color = Red; left  = Some l; parent = Some p                      }                   -> (p.right <- Some l; l.parent <- Some p)
	| {color = Red; right = Some r; parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some r; r.parent <- Some p)
	| {color = Red; right = Some r; parent = Some p                      }                   -> (p.right <- Some r; r.parent <- Some p)
	(* Red fully-leaf nodes can be deleted with no problem *)
	| {color = Red; right = None; left = None; parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- None)
	| {color = Red; right = None; left = None; parent = Some p                      }                   -> (p.right <- None)
	(* If the node is black, but the child is red, then the child can turn black and replace the node *)
	| {left  = Some ({color = Red} as l); parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some l; l.parent <- Some p; l.color <- Black)
	| {left  = Some ({color = Red} as l); parent = Some p                      }                   -> (p.right <- Some l; l.parent <- Some p; l.color <- Black)
	| {right = Some ({color = Red} as r); parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some r; r.parent <- Some p; r.color <- Black)
	| {right = Some ({color = Red} as r); parent = Some p                      }                   -> (p.right <- Some r; r.parent <- Some p; r.color <- Black)
	(* Bad things happen when both the node to be deleted and its child are black... *)
(*
	| {color = Black; left  = Some l; parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some l; l.parent <- Some p; delete_case_1 l)
	| {color = Black; left  = Some l; parent = Some p                      }                   -> (p.right <- Some l; l.parent <- Some p; delete_case_1 l)
	| {color = Black; right = Some r; parent = Some ({left = Some pl} as p)} as n when pl == n -> (p.left  <- Some r; r.parent <- Some p; delete_case_1 r)
	| {color = Black; right = Some r; parent = Some p                      }                   -> (p.right <- Some r; r.parent <- Some p; delete_case_1 r)
*)
	| n -> (
		delete_case_1 t n;
(*Printf.printf "DONE: %d\n" n.contents; pp2 n;*)
		(match n with
			| {left  = Some l; parent = Some ({left = Some pl} as p)} when pl == n -> (l.parent <- Some p; p.left  <- Some l)
			| {left  = Some l; parent = Some p                      }              -> (l.parent <- Some p; p.right <- Some l)
			| {left  = Some l; parent = None                        }              -> (l.parent <- None)
			| {right = Some r; parent = Some ({left = Some pl} as p)} when pl == n -> (r.parent <- Some p; p.left  <- Some r)
			| {right = Some r; parent = Some p                      }              -> (r.parent <- Some p; p.right <- Some r)
			| {right = Some r; parent = None                        }              -> (r.parent <- None)
			| {parent = Some ({left = Some pl} as p)} when pl == n -> (p.left  <- None)
			| {parent = Some p                      }              -> (p.right <- None)
			| n -> () (* Neither parent nor kids exist... this should have been taken care of by "remove" below... *)
		)
	)
;;

let remove_node t n = (
	match n with
	| {left = None; right = None; parent = None} -> (
		(* Root node, no other nodes *)
		t.nodes <- pred t.nodes;
		t.root <- None;
	)
	| {left = Some l; right = None; parent = None} -> (
		(* Root node, only the left kid exists *)
		(* Replace the root node with the left kid (and paint it black) *)
		l.color <- Black;
		l.parent <- None;
		t.nodes <- pred t.nodes;
		t.root <- Some l;
	)
	| {left = None; right = Some r; parent = None} -> (
		(* Root node, only the right kid exists *)
		(* Replace the root node with the right kid (and paint it black) *)
		r.color <- Black;
		r.parent <- None;
		t.nodes <- pred t.nodes;
		t.root <- Some r;
	)
	| ({left = Some l; right = Some r} as n) -> (
		(* Replace the contents with the next (or previous) node's contents *)
		let next = first_node_guts r in
		let prev = last_node_guts l in
		let (q,next_used) = match next with
			| {color = Red} | {left = Some _} | {right = Some _} -> (next, true)
			| _ -> (prev, false)
		in
		n.key <- q.key;
		n.contents <- q.contents;

		t.nodes <- pred t.nodes;
		
		(* Now delete q *)
		remove_leaf t q;
	)
	| n -> (
		(* The node has 0 or 1 kids *)
		t.nodes <- pred t.nodes;
		remove_leaf t n
	)
);;

let remove t key =
	match find_node t key with
	| None -> ()
	| Some n -> remove_node t n
;;

(*
let old_remove t key = (
	match find_node t key with
	| None -> () (* Not found! *)
	| Some {left = None; right = None; parent = None} -> (
		(* Root node, no other nodes *)
		t.nodes <- pred t.nodes;
		t.root <- None;
	)
	| Some {left = Some l; right = None; parent = None} -> (
		(* Root node, only the right kid exists *)
		(* Replace the root node with the right kid (and paint it black) *)
		l.color <- Black;
		l.parent <- None;
		t.nodes <- pred t.nodes;
		t.root <- Some l;
	)
	| Some {left = None; right = Some r; parent = None} -> (
		(* Root node, only the right kid exists *)
		(* Replace the root node with the right kid (and paint it black) *)
		r.color <- Black;
		r.parent <- None;
		t.nodes <- pred t.nodes;
		t.root <- Some r;
	)
	| Some ({left = Some l; right = Some r} as n) -> (
		(* Replace the contents with the next (or previous) node's contents *)
		let next = first_node_guts r in
		let prev = last_node_guts l in
		let (q,next_used) = match next with
			| {color = Red} | {left = Some _} | {right = Some _} -> (next, true)
			| _ -> (prev, false)
		in
(*		last_node_guts l in*)
(*		let q = first_node_guts r in*)
		n.key <- q.key;
		n.contents <- q.contents;

		t.nodes <- pred t.nodes;
		
		(* Now delete q *)
		remove_leaf t q;

(*		Printf.printf "%s was used!\n" (if next_used then "NEXT" else "PREV");*)
	)
	| Some n -> (
		(* The node has 0 or 1 kids *)
		t.nodes <- pred t.nodes;
		remove_leaf t n
	)
)
*)

let clear t =
	t.root <- None;
	t.nodes <- 0;
;;

(********)
(* TAKE *)
(********)
let take_first t =
	match first_node t with
	| None -> None
	| Some n -> (
		remove_node t n;
		Some (n.key, n.contents)
	)
;;
let take_last t =
	match last_node t with
	| None -> None
	| Some n -> (
		remove_node t n;
		Some (n.key, n.contents)
	)
;;
let take t key =
	match find_node t key with
	| None -> None
	| Some n -> (
		remove_node t n;
		Some (n.key, n.contents)
	)
;;

(****************)
(* SANITY CHECK *)
(****************)
(* Make sure the tree is a valid red/black tree *)
let sanity_check t = (
	match t.root with
	| None -> (
		if t.nodes = 0 then None else Some "Tree has no root, but \"nodes\" is not 0"
	)
	| Some {color = Red} -> (
		Some "Tree root node is red"
	)
	| Some r -> (
		(* Check if the kids of a red node are black *)
		(* Not tail-recursive, but I don't care *)
		let rec red_node_kids_are_black = function
			| {color = Red; left  = Some {color = Red}} -> false
			| {color = Red; right = Some {color = Red}} -> false
			| {left  = Some l; right = Some r} -> (red_node_kids_are_black l && red_node_kids_are_black r)
			| {left  = Some l} -> (red_node_kids_are_black l)
			| {right = Some r} -> (red_node_kids_are_black r)
			| _ -> true
		in
		if not (red_node_kids_are_black r) then (
			Some "There is a red node with at least one red child"
		) else (
			let rec black_height h = function
				| {color = Red; left = Some l; right = Some r} -> (
					match (black_height h l, black_height h r) with
					| (Some x, Some y) when x = y -> Some x
					| _ -> None
				)
				| {color = Black; left = Some l; right = Some r} -> (
					match (black_height (succ h) l, black_height (succ h) r) with
					| (Some x, Some y) when x = y -> Some x
					| _ -> None
				)
				| {color = Red; left = Some l} -> ( (* I don't think this can happen. It looks like red nodes have to either have both kids Some or both None *)
					None
				)
				| {color = Red; right = Some r} -> (
					None
				)
				| {color = Red; left = None; right = None} -> (
					Some h
				)
				| {color = Black; left = Some l} -> (
					match black_height (succ h) l with
					| Some x when x = (succ h) -> Some x
					| _ -> None
				)
				| {color = Black; right = Some r} -> (
					match black_height (succ h) r with
					| Some x when x = (succ h) -> Some x
					| _ -> None
				)
				| {color = Black; right = None; left = None} -> Some (succ h)
			in
			match (black_height 0 r) with
			| None -> Some "Not all leaves have the same black height"
			| Some h -> (
				let rec num_nodes = function
					| {left  = Some l; right = Some r} -> 1 + num_nodes l + num_nodes r
					| {left  = Some l} -> 1 + num_nodes l
					| {right = Some r} -> 1 + num_nodes r
					| _ -> 1
				in
				if num_nodes r <> t.nodes then (
					Some "Node count incorrect"
				) else (
					let rec order_check = function
						| (None, None, {key = k; left = Some l; right = Some r}) -> (
							if t.cmp l.key k < 0 && t.cmp k r.key < 0 then (
								order_check (None, Some k, l) && order_check (Some k, None, r)
							) else (false)
						)
						| (Some lb, None, {key = k; left = Some l; right = Some r}) -> (
							if t.cmp lb l.key < 0 && t.cmp l.key k < 0 && t.cmp k r.key < 0 then (
								order_check (Some lb, Some k, l) && order_check (Some k, None, r)
							) else (false)
						)
						| (None, Some rb, {key = k; left = Some l; right = Some r}) -> (
							if t.cmp l.key k < 0 && t.cmp k r.key < 0 && t.cmp r.key rb < 0 then (
								order_check (None, Some k, l) && order_check (Some k, Some rb, r)
							) else (false)
						)
						| (Some lb, Some rb, {key = k; left = Some l; right = Some r}) -> (
							if t.cmp lb l.key < 0 && t.cmp l.key k < 0 && t.cmp k r.key < 0 && t.cmp r.key rb < 0 then (
								order_check (Some lb, Some k, l) && order_check (Some k, Some rb, r)
							) else (false)
						)

						| (None, _, {key = k; left = Some l; right = None}) -> (
							if t.cmp l.key k < 0 then (
								order_check (None, Some k, l)
							) else (false)
						)
						| (_, None, {key = k; left = None; right = Some r}) -> (
							if t.cmp k r.key < 0 then (
								order_check (Some k, None, r)
							) else (false)
						)
						| (Some lb, _, {key = k; left = Some l; right = None}) -> (
							if t.cmp lb l.key < 0 && t.cmp l.key k < 0 then (
								order_check (Some lb, Some k, l)
							) else (false)
						)
						| (_, Some rb, {key = k; left = None; right = Some r}) -> (
							if t.cmp k r.key < 0 && t.cmp r.key rb < 0 then (
								order_check (Some k, Some rb, r)
							) else (false)
						)
						| (_, _, {left = None; right = None}) -> true
					in
					if order_check (None, None, r) then (
						None
					) else (
						Some "Nodes are not ordered"
					)
				)
			)
		)
	)
);;

(**************)
(* OTHER JUNK *)
(**************)
let rec black_height_rec so_far = function
	| {left = Some l; color = Black} -> black_height_rec (so_far + 1) l
	| {left = Some l; color = Red}   -> black_height_rec so_far l
	| {left = None;   color = Black} -> so_far + 1
	| {left = None;   color = Red}   -> so_far
;;
let black_height = function
	| {root = Some n} -> black_height_rec 0 n
	| {root = None} -> 0
;;

let rec max_height_rec = function
	| {left = Some l; right = Some r} -> 1 + max (max_height_rec l) (max_height_rec r)
	| {left = Some l; right = None  } -> 1 + max_height_rec l
	| {left = None;   right = Some r} -> 1 + max_height_rec r
	| {left = None;   right = None  } -> 1
;;
let max_height = function
	| {root = Some n} -> max_height_rec n
	| {root = None} -> 0
;;

let rec kpretty_print_rec k t n print_key print_val =
	let height = max_height t in
	let color_str = match n.color with
		| Red -> "r"
		| Black -> "b"
	in
	let rec make_string so_far c = (
		match c with
		| {parent = Some p; left  = Some l} when c == n -> make_string (" " ^ color_str ^ "-+" ^ so_far) p
		| {parent = Some p; right = Some r} when c == n -> make_string (" " ^ color_str ^ "-+" ^ so_far) p
		| {parent = Some p} when c == n -> make_string (" " ^ color_str ^ "--" ^ so_far) p
		| {parent = None; left  = Some _} | {parent = None; right = Some _} when c == n -> "#-+" ^ so_far
(*		| {parent = None; right = Some r} when c == n -> "#-+" ^ so_far*)
		| {parent = None} when c == n -> "#--" ^ so_far
		| {parent = Some p} when t.cmp c.key n.key < 0 && t.cmp n.key p.key < 0 -> make_string (" |" ^ so_far) p
		| {parent = Some p} when t.cmp c.key n.key > 0 && t.cmp n.key p.key > 0 -> make_string (" |" ^ so_far) p
		| {parent = Some p} -> make_string ("  " ^ so_far) p
		| {parent = None} -> " " ^ so_far
	) in
	let str = make_string "" n in
	Printf.kprintf k "%*s %s = %s\n" (-(2 * height + 1)) str (print_key n.key) (print_val n.contents);
	match prev_node_from n with
	| None -> ()
	| Some x -> kpretty_print_rec k t x print_key print_val
;;
let kpretty_print k t print_key print_val = match last_node t with
	| Some n -> kpretty_print_rec k t n print_key print_val
	| None -> (Printf.kprintf k "No nodes!\n")
;;
let pretty_print t print_key print_val = match last_node t with
	| Some n -> kpretty_print_rec (output_string stdout) t n print_key print_val
	| None -> (Printf.printf "No nodes!\n")
;;


(*******************************************************************************
(************)
(* DO STUFF *)
(************)
let a = create ();;

for i = 1 to 10 do
	Printf.printf "Add %d\n" i;
	add a i (i * 11);

	Printf.printf "done:\n";
	print_ordered a string_of_int string_of_int;

done;;

(*
List.iter (fun x ->
	Printf.printf "Add %d\n" x;
	add a x (x * 11);
	
	Printf.printf "done:\n";
	print_ordered a string_of_int string_of_int;
) [ 46;31;5;26;49;44;45;20;22;6;8;12;42;28;14;19;50;29;34;7;39;40;15;25;24;17;41;18;33;43;10;27;21;11;47;32;48;4;37;13;3;1;9;23;2;30;16;35;36;38 ];;
*)
print_ordered a string_of_int string_of_int;;

match find a 0 with
| None -> Printf.printf "0 not found!\n";
| Some (x,y) -> Printf.printf "0 found at (%d,%d)\n" x y
;;

(
	let f = 10 in
	match find a f with
	| None -> Printf.printf "%d not found!\n" f;
	| Some (x,y) -> Printf.printf "%d found at (%d,%d)\n" f x y
);;

iter (fun x y -> Printf.printf "%d = %d\n" x y) a;;



for i = 1 to 10 do
	match find_node a i with
	| None -> Printf.printf "WHAT!\n";
	| Some n -> (
		match next_node_from n with
		| None -> Printf.printf "Node %d=%d IS THE END!\n" n.key n.contents;
		| Some s -> Printf.printf "Node %d=%d has %d=%d as a right pal\n" n.key n.contents s.key s.contents;
	)
done;;
for i = 1 to 10 do
	match find_node a i with
	| None -> Printf.printf "WHAT!\n";
	| Some n -> (
		match prev_node_from n with
		| None -> Printf.printf "Node %d=%d IS THE BEGINNING!\n" n.key n.contents;
		| Some s -> Printf.printf "Node %d=%d has %d=%d as a left pal\n" n.key n.contents s.key s.contents;
	)
done;;


(* Test deletion *)
(*
let       sr = {parent = None; color =  Red ; key = 5; contents = 5; left = None; right = None};;
let     s    = {parent = None; color = Black; key = 4; contents = 4; left = None; right = None};;
let       sl = {parent = None; color = Black; key = 3; contents = 3; left = None; right = None};;
let   p      = {parent = None; color =  Red ; key = 2; contents = 2; left = None; right = None};;
let     n    = {parent = None; color = Black; key = 1; contents = 1; left = None; right = None};;
let g        = {parent = None; color = Black; key = 0; contents = 0; left = None; right = None};;

if true then (

	n.parent <- Some p;
	p.left <- Some n;

	s.parent <- Some p;
	p.right <- Some s;

	sl.parent <- Some s;
	s.left <- Some sl;

	sr.parent <- Some s;
	s.right <- Some sr;

	(* Can be either left or right *)
	p.parent <- Some g;
	g.right <- Some p;

) else (
	
	n.parent <- Some p;
	p.right <- Some n;

	s.parent <- Some p;
	p.left <- Some s;

	sl.parent <- Some s;
	s.right <- Some sl;

	sr.parent <- Some s;
	s.left <- Some sr;

	(* Can be either left or right *)
	p.parent <- Some g;
	g.left <- Some p;

);;

print_ordered {cmp = compare; root = Some g} string_of_int string_of_int;;

delete_case_6 n;;

print_ordered {cmp = compare; root = Some g} string_of_int string_of_int;;
*)

(* RL deletion test *)
(*
print_ordered a string_of_int string_of_int;;

remove a 9;;
add a 16 0;;
remove a 5;;
remove a 8;;
add a 5 0;;
(*
(match a with
	| {root = Some {right = Some {left = Some q}}} -> (q.color <- Red)
	| _ -> ()
);;
*)
print_ordered a string_of_int string_of_int;;

(match sanity_check a with
	| None -> Printf.printf "No problems, cap'n!\n"
	| Some x -> Printf.printf "PROBLEM! %s\n" x
);;
*)

(* BIG OLE TEST *)

let t = create ();;

let rec keep_going = function
	| 0 -> (Printf.printf "OK!\n"; print_ordered t string_of_int string_of_int)
	| x -> (
		let i = Random.int 100 in
		add t i x;
		match sanity_check t with
		| Some p -> (
			print_ordered t string_of_int string_of_int;
			Printf.printf "PROBLEM: %s\n" p
		)
		| None -> (
			for a = 1 to 4 do
				let j = Random.int 100 in
(*
				match find_node t j with
				| None -> ()
				| Some n -> remove_node t n
*)
				remove t j;
			done;

			keep_going (pred x)
		)
	)
;;

keep_going 100000;;

match take_first t with
	| Some (k1,c1) -> (
		Printf.printf "%d,%d TAKEN FROM " k1 c1;
		print_ordered t string_of_int string_of_int
	)
	| None -> ()
;;




(* Find test *)
let t = create ();;

add t 0 0;;
add t 5 5;;
add t 3 3;;
add t 7 7;;
add t 9 9;;
add t 4 4;;
add t 8 8;;

add t 10 10;;
add t 11 11;;
add t 12 12;;
add t 13 13;;
add t 14 14;;
add t 15 15;;
add t 16 16;;
add t 17 17;;
add t 18 18;;
add t 19 19;;
add t 20 20;;
add t 21 21;;
add t 22 22;;
add t 23 23;;
add t 24 24;;
add t 25 25;;
add t 26 26;;
add t 27 27;;
add t 28 28;;
add t 29 29;;
add t 30 30;;
add t 31 31;;
add t 32 32;;
add t 33 33;;
add t 34 34;;
add t 35 35;;
add t 36 36;;
add t 37 37;;
add t 38 38;;
add t 39 39;;
add t 40 40;;
add t 41 41;;
add t 42 42;;
add t 43 43;;
add t 44 44;;
add t 45 45;;
add t 46 46;;
add t 47 47;;
add t 48 48;;
add t 49 49;;


Printf.printf "%s\n" (fold_left (fun so_far k c -> so_far ^ " " ^ string_of_int k ^ "," ^ string_of_int c) "" t);;
Printf.printf "%s\n" (fold_right (fun so_far k c -> so_far ^ " " ^ string_of_int k ^ "," ^ string_of_int c) "" t);;

print_ordered t string_of_int string_of_int;;
Printf.printf "Max height %d; black height %d\n" (max_height t) (black_height t);;

pretty_print t (Printf.sprintf "%2d") (Printf.sprintf "%2d");;

(*
for a = 0 to 10 do
	(match find t a with
		| None -> Printf.printf "%d not found\n" a;
		| Some (b,c) -> Printf.printf "%d found at (%d,%d)\n" a b c;
	);
	(match find_largest_less_than t a with
		| None -> Printf.printf " None lesser\n";
		| Some (b,c) -> Printf.printf " Next smallest one is (%d,%d)\n" b c;
	);
	(match find_smallest_greater_than t a with
		| None -> Printf.printf " None greater\n";
		| Some (b,c) -> Printf.printf " Next largest one is (%d,%d)\n" b c;
	);
	(match find_smallest_not_less_than t a with
		| None -> Printf.printf " None not less than\n";
		| Some (b,c) -> Printf.printf " Next not less than is (%d,%d)\n" b c;
	);
	(match find_largest_not_greater_than t a with
		| None -> Printf.printf " None not greater than\n";
		| Some (b,c) -> Printf.printf " Next not greater than is (%d,%d)\n" b c;
	);
done;;
*)
*******************************************************************************)
