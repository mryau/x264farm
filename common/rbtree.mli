type ('a, 'b) t

val create : ?cmp:('a -> 'a -> int) -> unit -> ('a, 'b) t

val print : ('a, 'b) t -> ('a -> string) -> ('b -> string) -> unit
val print_ordered : ('a, 'b) t -> ('a -> string) -> ('b -> string) -> unit

val add : ('a, 'b) t -> 'a -> 'b -> unit

val find : ('a, 'b) t -> 'a -> ('a * 'b) option
val first : ('a, 'b) t -> ('a * 'b) option
val last : ('a, 'b) t -> ('a * 'b) option

val find_smallest_greater_than : ('a, 'b) t -> 'a -> ('a * 'b) option
val find_largest_less_than : ('a, 'b) t -> 'a -> ('a * 'b) option
val find_smallest_not_less_than : ('a, 'b) t -> 'a -> ('a * 'b) option
val find_largest_not_greater_than : ('a, 'b) t -> 'a -> ('a * 'b) option

val iter : ('a -> 'b -> unit) -> ('a, 'b) t -> unit

val fold_left : ('a -> 'b -> 'c -> 'a) -> 'a -> ('b, 'c) t -> 'a
val fold_right : ('a -> 'b -> 'c -> 'a) -> 'a -> ('b, 'c) t -> 'a

val remove : ('a, 'b) t -> 'a -> unit
val clear : ('a, 'b) t -> unit

val take_first : ('a, 'b) t -> ('a * 'b) option
val take_last : ('a, 'b) t -> ('a * 'b) option
val take : ('a, 'b) t -> 'a -> ('a * 'b) option

val sanity_check : ('a, 'b) t -> string option
val kpretty_print : (string -> unit) -> ('a, 'b) t -> ('a -> string) -> ('b -> string) -> unit
val pretty_print : ('a, 'b) t -> ('a -> string) -> ('b -> string) -> unit
