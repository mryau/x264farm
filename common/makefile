# Change this if compiling on Unixy OS (or run "make OBJ_EXT=.o")
OBJ_EXT=.obj


all_opt: pack.cmx net.cmx types.cmx rbtree.cmx list2.cmx matroska.cmx buffer2.cmx huff.cmx optimizations.cmx c-optimizations$(OBJ_EXT)

types.cmx: pack.cmx types.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa pack.cmx types.ml

pack.cmx: pack.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa pack.ml

net.cmx: types.cmx pack.cmx net.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa types.cmx pack.cmx net.ml


rbtree.cmx: rbtree.cmi rbtree.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa rbtree.ml

rbtree.cmi: rbtree.mli
	ocamlopt -c -thread rbtree.mli


list2.cmx: list2.cmi list2.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa list2.ml

list2.cmi: list2.mli
	ocamlopt -c -thread list2.mli


buffer2.cmx: buffer2.cmi buffer2.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa buffer2.ml

buffer2.cmi: buffer2.mli
	ocamlopt -c -thread buffer2.mli


huff.cmx: types.cmx huff.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa pack.cmx types.cmx huff.ml


matroska.cmx: matroska.ml
	ocamlopt -c -thread unix.cmxa threads.cmxa matroska.ml



# C OPTIMIZATIONS
optimizations.cmx: c-optimizations$(OBJ_EXT) optimizations.ml
	ocamlopt -c c-optimizations$(OBJ_EXT) optimizations.ml

c-optimizations$(OBJ_EXT): c-optimizations.c
	ocamlopt -c c-optimizations.c




test: all_opt test.ml
	ocamlopt -o test -thread unix.cmxa threads.cmxa str.cmxa pack.cmx types.cmx net.cmx btree.cmx buffer2.cmx huff.cmx test.ml
	test


clean:
	rm -rf ./*.exe
	rm -rf ./*.obj
	rm -rf ./*.cmi
	rm -rf ./*.cmx
	rm -rf ./*.cmo
	rm -rf ./*.cma
	rm -rf ./*.cmxa
	rm -rf ./*.lib
	rm -rf ./*.asm
