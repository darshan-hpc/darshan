/*
--------------------------------------------------------------------
lookup8.c, by Bob Jenkins, January 4 1997, Public Domain.
hash(), hash2(), hash3, and mix() are externally useful functions.
Routines to test the hash are included if SELF_TEST is defined.
You can use this free for any purpose.  It has no warranty.
--------------------------------------------------------------------
*/

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
typedef  unsigned long  long ub8;   /* unsigned 8-byte quantities */
typedef  unsigned long  int  ub4;   /* unsigned 4-byte quantities */
typedef  unsigned       char ub1;

#define hashsize(n) ((ub8)1<<(n))
#define hashmask(n) (hashsize(n)-1)

/*
--------------------------------------------------------------------
mix -- mix 3 64-bit values reversibly.
mix() takes 48 machine instructions, but only 24 cycles on a superscalar
  machine (like Intel's new MMX architecture).  It requires 4 64-bit
  registers for 4::2 parallelism.
All 1-bit deltas, all 2-bit deltas, all deltas composed of top bits of
  (a,b,c), and all deltas of bottom bits were tested.  All deltas were
  tested both on random keys and on keys that were nearly all zero.
  These deltas all cause every bit of c to change between 1/3 and 2/3
  of the time (well, only 113/400 to 287/400 of the time for some
  2-bit delta).  These deltas all cause at least 80 bits to change
  among (a,b,c) when the mix is run either forward or backward (yes it
  is reversible).
This implies that a hash using mix64 has no funnels.  There may be
  characteristics with 3-bit deltas or bigger, I didn't test for
  those.
--------------------------------------------------------------------
*/
#define mix64(a,b,c) \
{ \
  a -= b; a -= c; a ^= (c>>43); \
  b -= c; b -= a; b ^= (a<<9); \
  c -= a; c -= b; c ^= (b>>8); \
  a -= b; a -= c; a ^= (c>>38); \
  b -= c; b -= a; b ^= (a<<23); \
  c -= a; c -= b; c ^= (b>>5); \
  a -= b; a -= c; a ^= (c>>35); \
  b -= c; b -= a; b ^= (a<<49); \
  c -= a; c -= b; c ^= (b>>11); \
  a -= b; a -= c; a ^= (c>>12); \
  b -= c; b -= a; b ^= (a<<18); \
  c -= a; c -= b; c ^= (b>>22); \
}

/*
--------------------------------------------------------------------
darshan_hash() -- hash a variable-length key into a 64-bit value
  k     : the key (the unaligned variable-length array of bytes)
  len   : the length of the key, counting by bytes
  level : can be any 8-byte value
Returns a 64-bit value.  Every bit of the key affects every bit of
the return value.  No funnels.  Every 1-bit and 2-bit delta achieves
avalanche.  About 41+5len instructions.

The best hash table sizes are powers of 2.  There is no need to do
mod a prime (mod is sooo slow!).  If you need less than 64 bits,
use a bitmask.  For example, if you need only 10 bits, do
  h = (h & hashmask(10));
In which case, the hash table should have hashsize(10) elements.

If you are hashing n strings (ub1 **)k, do it like this:
  for (i=0, h=0; i<n; ++i) h = hash( k[i], len[i], h);

By Bob Jenkins, Jan 4 1997.  bob_jenkins@burtleburtle.net.  You may
use this code any way you wish, private, educational, or commercial,
but I would appreciate if you give me credit.

See http://burtleburtle.net/bob/hash/evahash.html
Use for hash table lookup, or anything where one collision in 2^^64
is acceptable.  Do NOT use for cryptographic purposes.
--------------------------------------------------------------------
*/

ub8 darshan_hash( k, length, level)
const register ub1 *k;        /* the key */
register ub8  length;   /* the length of the key */
register ub8  level;    /* the previous hash, or an arbitrary value */
{
  register ub8 a,b,c,len;

  /* Set up the internal state */
  len = length;
  a = b = level;                         /* the previous hash value */
  c = 0x9e3779b97f4a7c13LL; /* the golden ratio; an arbitrary value */

  /*---------------------------------------- handle most of the key */
  while (len >= 24)
  {
    a += (k[0]        +((ub8)k[ 1]<< 8)+((ub8)k[ 2]<<16)+((ub8)k[ 3]<<24)
     +((ub8)k[4 ]<<32)+((ub8)k[ 5]<<40)+((ub8)k[ 6]<<48)+((ub8)k[ 7]<<56));
    b += (k[8]        +((ub8)k[ 9]<< 8)+((ub8)k[10]<<16)+((ub8)k[11]<<24)
     +((ub8)k[12]<<32)+((ub8)k[13]<<40)+((ub8)k[14]<<48)+((ub8)k[15]<<56));
    c += (k[16]       +((ub8)k[17]<< 8)+((ub8)k[18]<<16)+((ub8)k[19]<<24)
     +((ub8)k[20]<<32)+((ub8)k[21]<<40)+((ub8)k[22]<<48)+((ub8)k[23]<<56));
    mix64(a,b,c);
    k += 24; len -= 24;
  }

  /*------------------------------------- handle the last 23 bytes */
  c += length;
  switch(len)              /* all the case statements fall through */
  {
  case 23: c+=((ub8)k[22]<<56);
  case 22: c+=((ub8)k[21]<<48);
  case 21: c+=((ub8)k[20]<<40);
  case 20: c+=((ub8)k[19]<<32);
  case 19: c+=((ub8)k[18]<<24);
  case 18: c+=((ub8)k[17]<<16);
  case 17: c+=((ub8)k[16]<<8);
    /* the first byte of c is reserved for the length */
  case 16: b+=((ub8)k[15]<<56);
  case 15: b+=((ub8)k[14]<<48);
  case 14: b+=((ub8)k[13]<<40);
  case 13: b+=((ub8)k[12]<<32);
  case 12: b+=((ub8)k[11]<<24);
  case 11: b+=((ub8)k[10]<<16);
  case 10: b+=((ub8)k[ 9]<<8);
  case  9: b+=((ub8)k[ 8]);
  case  8: a+=((ub8)k[ 7]<<56);
  case  7: a+=((ub8)k[ 6]<<48);
  case  6: a+=((ub8)k[ 5]<<40);
  case  5: a+=((ub8)k[ 4]<<32);
  case  4: a+=((ub8)k[ 3]<<24);
  case  3: a+=((ub8)k[ 2]<<16);
  case  2: a+=((ub8)k[ 1]<<8);
  case  1: a+=((ub8)k[ 0]);
    /* case 0: nothing left to add */
  }
  mix64(a,b,c);
  /*-------------------------------------------- report the result */
  return c;
}

/*
--------------------------------------------------------------------
 This works on all machines, is identical to hash() on little-endian 
 machines, and it is much faster than hash(), but it requires
 -- that the key be an array of ub8's, and
 -- that all your machines have the same endianness, and
 -- that the length be the number of ub8's in the key
--------------------------------------------------------------------
*/
ub8 hash2( k, length, level)
register ub8 *k;        /* the key */
register ub8  length;   /* the length of the key */
register ub8  level;    /* the previous hash, or an arbitrary value */
{
  register ub8 a,b,c,len;

  /* Set up the internal state */
  len = length;
  a = b = level;                         /* the previous hash value */
  c = 0x9e3779b97f4a7c13LL; /* the golden ratio; an arbitrary value */

  /*---------------------------------------- handle most of the key */
  while (len >= 3)
  {
    a += k[0];
    b += k[1];
    c += k[2];
    mix64(a,b,c);
    k += 3; len -= 3;
  }

  /*-------------------------------------- handle the last 2 ub8's */
  c += (length<<3);
  switch(len)              /* all the case statements fall through */
  {
    /* c is reserved for the length */
  case  2: b+=k[1];
  case  1: a+=k[0];
    /* case 0: nothing left to add */
  }
  mix64(a,b,c);
  /*-------------------------------------------- report the result */
  return c;
}

/*
--------------------------------------------------------------------
 This is identical to hash() on little-endian machines, and it is much
 faster than hash(), but a little slower than hash2(), and it requires
 -- that all your machines be little-endian, for example all Intel x86
    chips or all VAXen.  It gives wrong results on big-endian machines.
--------------------------------------------------------------------
*/

ub8 hash3( k, length, level)
register ub1 *k;        /* the key */
register ub8  length;   /* the length of the key */
register ub8  level;    /* the previous hash, or an arbitrary value */
{
  register ub8 a,b,c,len;

  /* Set up the internal state */
  len = length;
  a = b = level;                         /* the previous hash value */
  c = 0x9e3779b97f4a7c13LL; /* the golden ratio; an arbitrary value */

  /*---------------------------------------- handle most of the key */
  if (((size_t)k)&7)
  {
    while (len >= 24)
    {
      a += (k[0]        +((ub8)k[ 1]<< 8)+((ub8)k[ 2]<<16)+((ub8)k[ 3]<<24)
       +((ub8)k[4 ]<<32)+((ub8)k[ 5]<<40)+((ub8)k[ 6]<<48)+((ub8)k[ 7]<<56));
      b += (k[8]        +((ub8)k[ 9]<< 8)+((ub8)k[10]<<16)+((ub8)k[11]<<24)
       +((ub8)k[12]<<32)+((ub8)k[13]<<40)+((ub8)k[14]<<48)+((ub8)k[15]<<56));
      c += (k[16]       +((ub8)k[17]<< 8)+((ub8)k[18]<<16)+((ub8)k[19]<<24)
       +((ub8)k[20]<<32)+((ub8)k[21]<<40)+((ub8)k[22]<<48)+((ub8)k[23]<<56));
      mix64(a,b,c);
      k += 24; len -= 24;
    }
  }
  else
  {
    while (len >= 24)    /* aligned */
    {
      a += *(ub8 *)(k+0);
      b += *(ub8 *)(k+8);
      c += *(ub8 *)(k+16);
      mix64(a,b,c);
      k += 24; len -= 24;
    }
  }

  /*------------------------------------- handle the last 23 bytes */
  c += length;
  switch(len)              /* all the case statements fall through */
  {
  case 23: c+=((ub8)k[22]<<56);
  case 22: c+=((ub8)k[21]<<48);
  case 21: c+=((ub8)k[20]<<40);
  case 20: c+=((ub8)k[19]<<32);
  case 19: c+=((ub8)k[18]<<24);
  case 18: c+=((ub8)k[17]<<16);
  case 17: c+=((ub8)k[16]<<8);
    /* the first byte of c is reserved for the length */
  case 16: b+=((ub8)k[15]<<56);
  case 15: b+=((ub8)k[14]<<48);
  case 14: b+=((ub8)k[13]<<40);
  case 13: b+=((ub8)k[12]<<32);
  case 12: b+=((ub8)k[11]<<24);
  case 11: b+=((ub8)k[10]<<16);
  case 10: b+=((ub8)k[ 9]<<8);
  case  9: b+=((ub8)k[ 8]);
  case  8: a+=((ub8)k[ 7]<<56);
  case  7: a+=((ub8)k[ 6]<<48);
  case  6: a+=((ub8)k[ 5]<<40);
  case  5: a+=((ub8)k[ 4]<<32);
  case  4: a+=((ub8)k[ 3]<<24);
  case  3: a+=((ub8)k[ 2]<<16);
  case  2: a+=((ub8)k[ 1]<<8);
  case  1: a+=((ub8)k[ 0]);
    /* case 0: nothing left to add */
  }
  mix64(a,b,c);
  /*-------------------------------------------- report the result */
  return c;
}

#ifdef SELF_TEST

/* used for timings */
void driver1()
{
  ub8 buf[256];
  ub8 i;
  ub8 h=0;

  for (i=0; i<256; ++i) 
  {
    h = darshan_hash(buf,i,h);
  }
}

/* check that every input bit changes every output bit half the time */
#define HASHSTATE 1
#define HASHLEN   1
#define MAXPAIR 80
#define MAXLEN 5
void driver2()
{
  ub1 qa[MAXLEN+1], qb[MAXLEN+2], *a = &qa[0], *b = &qb[1];
  ub8 c[HASHSTATE], d[HASHSTATE], i, j=0, k, l, m, z;
  ub8 e[HASHSTATE],f[HASHSTATE],g[HASHSTATE],h[HASHSTATE];
  ub8 x[HASHSTATE],y[HASHSTATE];
  ub8 hlen;

  printf("No more than %d trials should ever be needed \n",MAXPAIR/2);
  for (hlen=0; hlen < MAXLEN; ++hlen)
  {
    z=0;
    for (i=0; i<hlen; ++i)  /*----------------------- for each byte, */
    {
      for (j=0; j<8; ++j)   /*------------------------ for each bit, */
      {
	for (m=0; m<8; ++m) /*-------- for serveral possible levels, */
	{
	  for (l=0; l<HASHSTATE; ++l) e[l]=f[l]=g[l]=h[l]=x[l]=y[l]=~((ub8)0);

      	  /*---- check that every input bit affects every output bit */
	  for (k=0; k<MAXPAIR; k+=2)
	  { 
	    ub8 finished=1;
	    /* keys have one bit different */
	    for (l=0; l<hlen+1; ++l) {a[l] = b[l] = (ub1)0;}
	    /* have a and b be two keys differing in only one bit */
	    a[i] ^= (k<<j);
	    a[i] ^= (k>>(8-j));
	     c[0] = darshan_hash(a, hlen, m);
	    b[i] ^= ((k+1)<<j);
	    b[i] ^= ((k+1)>>(8-j));
	     d[0] = darshan_hash(b, hlen, m);
	    /* check every bit is 1, 0, set, and not set at least once */
	    for (l=0; l<HASHSTATE; ++l)
	    {
	      e[l] &= (c[l]^d[l]);
	      f[l] &= ~(c[l]^d[l]);
	      g[l] &= c[l];
	      h[l] &= ~c[l];
	      x[l] &= d[l];
	      y[l] &= ~d[l];
	      if (e[l]|f[l]|g[l]|h[l]|x[l]|y[l]) finished=0;
	    }
	    if (finished) break;
	  }
	  if (k>z) z=k;
	  if (k==MAXPAIR) 
	  {
	     printf("Some bit didn't change: ");
	     printf("%.8lx %.8lx %.8lx %.8lx %.8lx %.8lx  ",
	            e[0],f[0],g[0],h[0],x[0],y[0]);
	     printf("i %ld j %ld m %ld len %ld\n",
	            (ub4)i,(ub4)j,(ub4)m,(ub4)hlen);
	  }
	  if (z==MAXPAIR) goto done;
	}
      }
    }
   done:
    if (z < MAXPAIR)
    {
      printf("Mix success  %2ld bytes  %2ld levels  ",(ub4)i,(ub4)m);
      printf("required  %ld  trials\n",(ub4)(z/2));
    }
  }
  printf("\n");
}

/* Check for reading beyond the end of the buffer and alignment problems */
void driver3()
{
  ub1 buf[MAXLEN+20], *b;
  ub8 len;
  ub1 q[] = "This is the time for all good men to come to the aid of their country";
  ub1 qq[] = "xThis is the time for all good men to come to the aid of their country";
  ub1 qqq[] = "xxThis is the time for all good men to come to the aid of their country";
  ub1 qqqq[] = "xxxThis is the time for all good men to come to the aid of their country";
  ub1 o[] = "xxxxThis is the time for all good men to come to the aid of their country";
  ub1 oo[] = "xxxxxThis is the time for all good men to come to the aid of their country";
  ub1 ooo[] = "xxxxxxThis is the time for all good men to come to the aid of their country";
  ub1 oooo[] = "xxxxxxxThis is the time for all good men to come to the aid of their country";
  ub8 h,i,j,ref,x,y;

  printf("Endianness.  These should all be the same:\n");
  h = darshan_hash(q+0, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(qq+1, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(qqq+2, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(qqqq+3, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(o+4, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(oo+5, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(ooo+6, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  h = darshan_hash(oooo+7, (ub8)(sizeof(q)-1), (ub8)0);
  printf("%.8lx%.8lx\n", (ub4)h, (ub4)(h>>32));
  printf("\n");
  for (h=0, b=buf+1; h<8; ++h, ++b)
  {
    for (i=0; i<MAXLEN; ++i)
    {
      len = i;
      for (j=0; j<i; ++j) *(b+j)=0;

      /* these should all be equal */
      ref = darshan_hash(b, len, (ub8)1);
      *(b+i)=(ub1)~0;
      *(b-1)=(ub1)~0;
      x = darshan_hash(b, len, (ub8)1);
      y = darshan_hash(b, len, (ub8)1);
      if ((ref != x) || (ref != y)) 
      {
	printf("alignment error: %.8lx %.8lx %.8lx %ld %ld\n",ref,x,y,h,i);
      }
    }
  }
}

/* check for problems with nulls */
 void driver4()
{
  ub1 buf[1];
  ub8 h,i,state[HASHSTATE];


  buf[0] = ~0;
  for (i=0; i<HASHSTATE; ++i) state[i] = 1;
  printf("These should all be different\n");
  for (i=0, h=0; i<8; ++i)
  {
    h = darshan_hash(buf, (ub8)0, h);
    printf("%2ld  0-byte strings, darshan_hash is  %.8lx%.8lx\n", (ub4)i,
      (ub4)h,(ub4)(h>>32));
  }
}


int main()
{
  driver1();   /* test that the key is hashed: used for timings */
  driver2();   /* test that whole key is hashed thoroughly */
  driver3();   /* test that nothing but the key is hashed */
  driver4();   /* test hashing multiple buffers (all buffers are null) */
  return 1;
}

#endif  /* SELF_TEST */
