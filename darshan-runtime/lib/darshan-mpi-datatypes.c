/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*  
 *  (C) 2001 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */
//#include "mpi.h"
#include "adio.h"
#include "adio_extern.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>



ADIOI_Flatlist_node * get_flatlist(MPI_Datatype dt){
      int is_contig;
      ADIOI_Datatype_iscontig(dt,&is_contig);
      if (is_contig)
	  return NULL;
      else {
	  ADIOI_Flatlist_node *flat_buf;
	  ADIOI_Flatten_datatype(dt);
	  flat_buf = ADIOI_Flatlist;
	  while ((flat_buf)&&(flat_buf->type != dt)) flat_buf = flat_buf->next; 
	  if (flat_buf) {
	      if (flat_buf->incr_size[0] == -1) {
		  int i;
		  flat_buf->incr_size[0] = 0;
		  for (i=1;i<flat_buf->count;i++)
		      flat_buf->incr_size[i] = flat_buf->incr_size[i-1] + flat_buf->blocklens[i-1]; 
	      }
	  }
	  return flat_buf;
      }
}
 
void print_flatlist(MPI_Datatype dt) {
  int b_index;
  ADIOI_Flatlist_node *flat_buf;
  flat_buf = get_flatlist(dt);
  if (flat_buf) {
    printf("FLATLIST with %lld elements\n",(long long) flat_buf->count);	
    for (b_index=0; b_index < flat_buf->count; b_index++) {
	printf ("%3d-th: offset=%8lld len=%8lld \n",b_index, flat_buf->indices[b_index],flat_buf->blocklens[b_index]);
    }
  }
  else
    printf ("NULL flat_buf\n");
}

void print_flatlist2(ADIOI_Flatlist_node *flat_buf) {
  int b_index;
  if (flat_buf)
    for (b_index=0; b_index < flat_buf->count; b_index++) {
	printf ("%3d-th: offset=%8lld len=%8lld \n",b_index, flat_buf->indices[b_index],flat_buf->blocklens[b_index]);
    }
  else
    printf ("NULL flat_buf\n");
}


#define LEFT 0
#define RIGHT 1

// assumes : x > 0
// result:  between 0 and flat_buf->count inclusively
// returns the index of the flat_buf where x is located
// LEFT
//  ____***____***____****--
//  000000011111112222222233
//  RIGHT
//  000011111112222222333333 

int find_bin_search(MPI_Offset x, MPI_Datatype datatype, int leftright){
    ADIOI_Flatlist_node *flat_buf = get_flatlist(datatype);
    if (!flat_buf) return 0;

    int l = 0 ,r = flat_buf->count - 1 ,found = -1;
    while (l <= r) {
	int m = (l + r) / 2;
	if (x < flat_buf->indices[m] - flat_buf->indices[0] + flat_buf->blocklens[m]) {
	    if (l == m) {
		found = m;
		break;
	    }
	    if (x >= flat_buf->indices[m - 1] - flat_buf->indices[0] + flat_buf->blocklens[m - 1]) {
		found = m;
		break;
	    }
	    r = m - 1 ;
	}
	else {
	    if (m == r) {
		found = m +1;
		//found = m;
		break;
	    }
	    if (x < flat_buf->indices[m + 1] - flat_buf->indices[0]) {
		found = m + 1;
		//found = m;
		break;
	    }
	    l = m +1;
	}
    }
    //    return found - ((flat_buf->blocklens[0] == 0)?1:0);
    if ((leftright == RIGHT) 
	&& (found < flat_buf->count)
	&& (x < flat_buf->indices[found] - flat_buf->indices[0] + flat_buf->blocklens[found]) 
	&& (x >= flat_buf->indices[found] - flat_buf->indices[0])) found++;
    return found;
}

// Finds the file offset within the extent range  
// assumes : x > 0
MPI_Offset find_size_bin_search(MPI_Offset x, ADIOI_Flatlist_node *flat_buf){
  int l = 0 ,r = flat_buf->count - 1;
  MPI_Offset size;

  while (l <= r) {
    int m = (l + r) / 2;
    //printf("\n*** l=%d r=%d\n",l,r);
    if (x < flat_buf->incr_size[m] + flat_buf->blocklens[m]) {
      if (l == m) {
	size = flat_buf->indices[m] + x - flat_buf->incr_size[m];
	break;
      }
      if (x >= flat_buf->incr_size[m]) {
	size = flat_buf->indices[m] + x - flat_buf->incr_size[m];
	break;
      }
      r = m - 1 ;
    }
    else {
	 // ???
	if (m == r) {
	    size = flat_buf->indices[m] + x - flat_buf->incr_size[m];
	    break;
	}
	if (x < flat_buf->incr_size[m + 1] + flat_buf->blocklens[m + 1]) {
	    size = flat_buf->indices[m + 1] + x - flat_buf->incr_size[m + 1];
	    break;
	}
	l = m +1;
    }
  }
  return size;
}

void free_datatype(MPI_Datatype type) {
    int ni, na, nd, combiner; 
    MPI_Type_get_envelope(type, &ni, &na, &nd, &combiner); 
    if (combiner != MPI_COMBINER_NAMED)
	MPI_Type_free(&type);
}

/*int my_get_view(MPI_File fh, MPI_Offset *disp, MPI_Datatype *etype, MPI_Datatype *filetype){
    ADIO_File adio_fh = MPIO_File_resolve(fh);
    *disp = adio_fh->disp;
    *etype =  adio_fh->etype;
    *filetype = adio_fh->filetype;
    return MPI_SUCCESS;
}
*/

MPI_Offset func_1_per( MPI_Offset x, ADIOI_Flatlist_node *flat_buf){
  return  find_size_bin_search(x, flat_buf) - flat_buf->indices[0];
}

/* Maps a view offset on a file offset 
Warning: offset is in multiple of etypes, needed func_1_inf for converting the x into byte offset before computing a mapping for x + number_of_bytes
*/

MPI_Offset func_1(MPI_File fh, MPI_Offset x){
    int is_contig, etype_size;
    MPI_Offset disp_plus_lb, ret;
    MPI_Datatype etype, filetype;
    char datarep[MPI_MAX_DATAREP_STRING];
    MPI_Aint lb, extent;
    
    MPI_File_get_view(fh, &disp_plus_lb, &etype, &filetype, datarep);
    //my_get_view(fh, &disp_plus_lb, &etype, &filetype);
    MPI_Type_get_extent(filetype, &lb, &extent);
    MPI_Type_size(etype, &etype_size);
    
    x = x * etype_size; 
    
    disp_plus_lb += lb;
    ADIOI_Datatype_iscontig(filetype, &is_contig);
    if (is_contig) 
	ret = x + disp_plus_lb;
    else {
	int sz;
	MPI_Type_size(filetype, &sz);
	ret = disp_plus_lb + x / sz * extent + func_1_per(x % sz, get_flatlist(filetype));
    }
    free_datatype(etype);
    free_datatype(filetype);
    return ret;
}

/* 
Warning: offset is in multiple of etypes, needed func_1_inf for converting the x into byte offset before computing a mapping for x + number_of_bytes

Maps a view offset on a file offset: 
trasforms first etype -> bytes 
and then moves on the previous byte 

*/

MPI_Offset func_1_inf(MPI_File fh, MPI_Offset x, int memtype_size){
    int is_contig, etype_size;
    MPI_Offset disp_plus_lb, ret;
    MPI_Datatype etype, filetype;
    char datarep[MPI_MAX_DATAREP_STRING];
    MPI_Aint lb, extent;
    
    MPI_File_get_view(fh, &disp_plus_lb, &etype, &filetype, datarep);
    //my_get_view(fh, &disp_plus_lb, &etype, &filetype);
    MPI_Type_get_extent(filetype, &lb, &extent);
    MPI_Type_size(etype, &etype_size);
    
    x = x * etype_size + memtype_size - 1; 
    
    disp_plus_lb += lb;
    ADIOI_Datatype_iscontig(filetype, &is_contig);
    if (is_contig) 
	ret = x + disp_plus_lb;
    else {
	int sz;
	MPI_Type_size(filetype, &sz);
	ret = disp_plus_lb + x / sz * extent + func_1_per(x % sz, get_flatlist(filetype));
    }
    free_datatype(etype);
    free_datatype(filetype);
    return ret;
}


/* This version compacts the neighboring contiguous blocks */ 
int count_contiguous_blocks_memory(MPI_Datatype datatype, int count) {
    ADIOI_Flatlist_node *flat_buf;
    flat_buf = get_flatlist(datatype);
    return (flat_buf)?(flat_buf->count*count):1;
}

#define BEGIN_BLOCK(x,block_size) (((x)/(block_size))*(block_size))
#define END_BLOCK(x,block_size) (((x)/(block_size)+1)*(block_size)-1)
#define BEGIN_NEXT_BLOCK(x,block_size) (((x)/(block_size)+1)*(block_size))


int count_contiguous_blocks_file(MPI_File fh, MPI_Offset foff1, MPI_Offset foff2) {
    MPI_Offset disp;
    MPI_Datatype etype, filetype;
    char datarep[MPI_MAX_DATAREP_STRING];
    int is_contig;
    int ret;

    assert(foff1 <= foff2);
     
    MPI_File_get_view(fh, &disp, &etype, &filetype, datarep);
    //my_get_view(fh, &disp, &etype, &filetype);
    ADIOI_Datatype_iscontig(filetype, &is_contig);
    if (is_contig)
	ret=1;
    else { 
	MPI_Aint extent, lb;
	ADIOI_Flatlist_node *flat_buf;
	flat_buf = get_flatlist(filetype);
	MPI_Type_get_extent(filetype, &lb, &extent);

	if (disp+lb > foff1) {
		printf("disp=%lld lb=%lld foff1=%lld foff2=%lld\n", disp, (long long) lb, foff1, foff2);
		print_flatlist(filetype);		
	}	
	assert(disp+lb <= foff1);
	assert(lb == flat_buf->indices[0]);

	if (foff2 < ((flat_buf->blocklens[0] == 0)?flat_buf->indices[1]:flat_buf->indices[0]))
	    ret=0;
	else{
	    int ind1, ind2;
	    ind1 = find_bin_search((foff1 - disp - lb) % extent, filetype, LEFT);
	    ind2 = find_bin_search((foff2 - disp - lb) % extent, filetype, RIGHT);
	    
	    if ((foff2 - disp - lb) < BEGIN_NEXT_BLOCK(foff1 - disp - lb, extent)) 
		ret = ind2 - ind1;
	    else 
		ret = flat_buf->count - ind1 - ((flat_buf->blocklens[flat_buf->count-1] == 0)?1:0)+
		    (BEGIN_BLOCK(foff2 - disp - lb, extent)-
		     BEGIN_NEXT_BLOCK(foff1 - disp - lb, extent)) 
		    / extent * (flat_buf->count-
				((flat_buf->blocklens[0] == 0)?1:0) -
				((flat_buf->blocklens[flat_buf->count-1] == 0)?1:0)-
				(((flat_buf->blocklens[flat_buf->count-1])&&(flat_buf->blocklens[flat_buf->count-1] + flat_buf->indices[flat_buf->count-1] - flat_buf->indices[0] == extent ))?1:0))
		    + ind2 - (((ind2>0)&&(flat_buf->blocklens[0] == 0))?1:0) -  
		    (((ind2>0)&&(flat_buf->indices[0]==0))?1:0); 
	}
    }
    free_datatype(etype);
    free_datatype(filetype);
    return ret;
}

