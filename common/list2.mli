(*******************************************************************************
	This file is a part of x264farm.

	mp3packer is free software; you can redistribute it and/or modify
	it under the terms of the GNU General Public License as published by
	the Free Software Foundation; either version 2 of the License, or
	(at your option) any later version.

	mp3packer is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU General Public License for more details.

	You should have received a copy of the GNU General Public License
	along with mp3packer; if not, write to the Free Software
	Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*******************************************************************************)

exception Empty
(** Raised when a function like {!pop_last} is used on an empty list *)

type 'a t
(** The type of doubly-linked lists *)

val create : unit -> 'a t
(** Creates an empty doubly-linked list *)

val clear : 'a t -> unit
(** Discard all elements from a list *)

val length : 'a t -> int
(** The number of elements of a list *)

val is_empty : 'a t -> bool
(** [true] if the list is empty, otherwise [false] *)

val append : 'a t -> 'a -> unit
(** [append q x] adds [x] to the end of list [q] *)

val prepend : 'a t -> 'a -> unit
(** [prepend q x] adds [x] to the beginning of list [q] *)

val iter : ('a -> 'b) -> 'a t -> unit
(** [iter f q] applies [f] to all elements of [q], from beginning to end *)

val rev_iter : ('a -> 'b) -> 'a t -> unit
(** [iter f q] applies [f] to all elements of [q] backwards, from end to beginning *)

val iteri : (int -> 'a -> 'b) -> 'a t -> unit
val rev_iteri : (int -> 'a -> 'b) -> 'a t -> unit
(** Same as {!iter} and {!rev_iter}, respectively, but the first argument is the index.
	Note that the index of rev_iteri counts down *)

val reverse : 'a t -> 'a t
(** Returns a list with all the elements reversed *)

val pop_last : 'a t -> 'a
val take_last : 'a t -> 'a
(** Take the last element out of the list and return it *)

val pop_first : 'a t -> 'a
val take_first : 'a t -> 'a
(** Take the first element out of the list and return it *)

val fold : ('a -> 'b -> 'a) -> 'a -> 'b t -> 'a
(** [fold f i q] is the same as [(f ... (f (f i first) second) ... last)] *)

val rev_fold : ('a -> 'b -> 'a) -> 'a -> 'b t -> 'a
(** [rev_fold f i q] is the same as [(f ... (f (f i last) second_last) ... first)] *)

val nth : 'a t -> int -> 'a
(** [nth q n] returns the [n]th element of [q]. O([length q]) *)

val peek_first : 'a t -> 'a
val head : 'a t -> 'a
(** Returns the first element of the list without changing the contonts *)

val peek_last : 'a t -> 'a
val tail : 'a t -> 'a
(** Returns the last element of the list without changing the contonts *)

val map : ('a -> 'b) -> 'a t -> 'b t
(** Applies the function to each of the elements and returns the resultant list. *)

val rev_map : ('a -> 'b) -> 'a t -> 'b t
(** Same as above, but the resultant list is reversed *)

val of_array : 'a array -> 'a t
val to_array : 'a t -> 'a array
val of_list : 'a list -> 'a t
val to_list : 'a t -> 'a list
(** Converts between doubly-linked lists and standard data types *)
