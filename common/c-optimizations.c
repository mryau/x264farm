#include <caml/mlvalues.h>
#include <caml/memory.h>

#define min(X, Y)  ((X) < (Y) ? (X) : (Y))

unsigned char paeth_guts(unsigned char a, unsigned char b, unsigned char c) {
	int p = (int)a + (int)b - (int)c;
	int pa = abs(p - a);
	int pb = abs(p - b);
	int pc = abs(p - c);

	if(pa <= pb && pa <= pc) {
		return a;
	} else if(pb <= pc) {
		return b;
	} else {
		return c;
	}
}

#if 1
// Optimized
/* Does an in-place calculation of the Paeth predictor, based on the dimensions (x,y), total pixels (t=x*y) and offset o */
void do_paeth(value val_str, value val_x, value val_o) {
	CAMLparam3 (val_str, val_x, val_o);
	int dim_x = Int_val(val_x);
	int offset = min(Int_val(val_o),string_length(val_str)); /* Make sure the offset doesn't get out of the string */
	int total = string_length(val_str) - offset;
	int dim_y = total / dim_x; /* This is done so that C doesn't get out of the string and screw up everything */
	unsigned char *str = String_val(val_str) + offset;

	int x;
	int y;
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char p;
	for(y = dim_y - 1; y >= 1; y--) {
		for(x = dim_x - 1; x >= 1; x--) {
			a = str[y * dim_x + (x - 1)];
			b = str[(y - 1) * dim_x + x];
			c = str[(y - 1) * dim_x + (x - 1)];
			str[y * dim_x + x] = str[y * dim_x + x] - paeth_guts(a,b,c);
//			printf("PAETH(%d,%d,%d) = %d\n",a,b,c,paeth_guts(a,b,c));
		}

		/* x = 0 */
		str[y * dim_x] = str[y * dim_x] - str[(y - 1) * dim_x];
	}

	/* y = 0 */
	for(x = dim_x - 1; x >= 1; x--) {
		str[x] = str[x] - str[(x - 1)];
	}

	/* x = 0, y = 0 */
	/* Unchanged! */

	CAMLreturn0;
}

#else
// NORMAL
void do_paeth(value val_str, value val_x, value val_o) {
	CAMLparam3 (val_str, val_x, val_o);
	int dim_x = Int_val(val_x);
	int offset = min(Int_val(val_o),string_length(val_str)); /* Make sure the offset doesn't get out of the string */
	int total = string_length(val_str) - offset;
	int dim_y = total / dim_x; /* This is done so that C doesn't get out of the string and screw up everything */
	unsigned char *str = String_val(val_str) + offset;

	int x;
	int y;
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char p;
	for(y = dim_y - 1; y >= 0; y--) {
		for(x = dim_x - 1; x >= 0; x--) {
			a = (x == 0) ? (0) : (str[y * dim_x + (x - 1)]);
			b = (y == 0) ? (0) : (str[(y - 1) * dim_x + x]);
			c = (x == 0 || y == 0) ? (0) : (str[(y - 1) * dim_x + (x - 1)]);
			str[y * dim_x + x] = str[y * dim_x + x] - paeth_guts(a,b,c);
//			printf("PAETH(%d,%d,%d) = %d\n",a,b,c,paeth_guts(a,b,c));
		}
	}

	CAMLreturn0;
}

#endif


void undo_paeth(value val_str, value val_x, value val_o) {
	CAMLparam3(val_str, val_x, val_o);
	int dim_x = Int_val(val_x);
	int offset = min(Int_val(val_o),string_length(val_str));
	int total = string_length(val_str) - offset;
	int dim_y = total / dim_x;
	unsigned char *str = String_val(val_str) + offset;

	int x;
	int y;
	unsigned char a;
	unsigned char b;
	unsigned char c;
	unsigned char p;
	/* y = 0 */
	for(x = 1; x < dim_x; x++) {
		str[x] = str[x] + str[(x - 1)];
	}

	for(y = 1; y < dim_y; y++) {
		/* x = 0 */
		str[y * dim_x] = str[y * dim_x] + str[(y - 1) * dim_x];

		for(x = 1; x < dim_x; x++) {
			a = str[y * dim_x + (x - 1)];
			b = str[(y - 1) * dim_x + x];
			c = str[(y - 1) * dim_x + (x - 1)];
			str[y * dim_x + x] = str[y * dim_x + x] + paeth_guts(a,b,c);
		}

	}


	/* x = 0, y = 0 */
	/* Unchanged! */

	CAMLreturn0;
}

