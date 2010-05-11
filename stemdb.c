#include <Python.h>
#include <fcntl.h>
#include <math.h>
#include <stddef.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <dirent.h>


/* 

=== TODO ===

- stemdbtype.add_key(name,[col1,col2,...],nuniques=None)
    - further inserts update index (or fail if nvals > uniques and return indexes)
- stemdbtype[x] = [val1,val2,...]
- stemdbtype.delete(key_name,[val1,val2,...]) 
    - deletes from data and key index
- stemdb.delete_key
- Support anonymous mmap
- Support greater than, less than
- Support floating point data
- Support binary thats not byte-aligned
- stemdbtype.get_index(key_name,[val1,val2,...]) => [index1,index2,...]
- Support file sizes greater than memory? or let OS page?
- support locking / multi-threading / multiple reader procs or writers
- server mode
- horizontal scaling
- transactions?

*/

typedef struct {
  char name[32];

  void *data;
  void *meta;
  uint64_t *free;

  int data_fd;
  int meta_fd;
  int free_fd;

  uint64_t data_size;
  uint64_t meta_size;
  uint64_t free_size;

  uint64_t many;

} stemdb_key;

typedef struct {
  PyObject_HEAD
  char *dir_name;

  void *data;
  void *meta;
  uint64_t *free;

  int data_fd;
  int meta_fd;
  int free_fd;

  uint64_t data_size;
  uint64_t meta_size;
  uint64_t free_size;

  uint16_t num_cols;
  uint32_t row_size;
  uint32_t row_bytes;

  stemdb_key **keys;
  uint16_t num_keys;

} PyStemDBObject;



static PyTypeObject PyStemDBType;

typedef struct meta_col {
  char name[32];
  char type;
  uint16_t num_bits;
  uint32_t bit_offset;
} __attribute__((packed)) meta_col;

#ifndef O_NOATIME
#define O_NOATIME 0
#endif


#define META_NUM_COLS_OFFSET 0
#define META_ROW_SIZE_OFFSET 2
#define META_NUM_ROWS_OFFSET 6
#define META_NUM_FREE_ROWS_OFFSET 14
#define META_HEADER_SIZE 22

#define META_KEY_NUM_COLS_OFFSET 0
#define META_KEY_NUM_NODES_OFFSET 2
#define META_KEY_NUM_FREE_NODES_OFFSET 10
#define META_KEY_MANY_OFFSET 18
#define META_KEY_COLS_OFFSET 26


#define NODE_SIZE 10
#define ALLOCATION 4096 // file size increment
#define RESERVE 0x100000000000L // max data file size

#define BITFIELD(SIZE, NAME) unsigned char NAME[(SIZE) / 8 + ((SIZE) % 8 != 0)]

static inline void setbit(unsigned char field[], size_t idx)
{ field[idx / 8] |= 1u << (idx % 8); }

static inline void unsetbit(unsigned char field[], size_t idx)
{ field[idx / 8] &= ~(1u << (idx % 8)); }

static inline unsigned char isbitset(unsigned char field[], size_t idx)
{ return field[idx / 8] & (1u << (idx % 8)); }

static inline unsigned char is_linked_list(void *node) {
  return ((unsigned char *)node)[0] & 4;
}

static inline unsigned char left_is_leaf_node(void *node) {
  return ((unsigned char *)node)[0] & 1;
}

static inline unsigned char right_is_leaf_node(void *node) {
  return ((unsigned char *)node)[0] & 2;
}

static inline void set_linked_list(void *node) {
  ((unsigned char *)node)[0] |= 4;
}

static inline void left_set_leaf_node(void *node) {
  ((unsigned char *)node)[0] |= 1;
}

static inline void right_set_leaf_node(void *node) {
  ((unsigned char *)node)[0] |= 2;
}

static inline void unset_linked_list(void *node) {
  ((unsigned char *)node)[0] &= ~4;
}

static inline void left_unset_leaf_node(void *node) {
  ((unsigned char *)node)[0] &= ~1;
}

static inline void right_unset_leaf_node(void *node) {
  ((unsigned char *)node)[0] &= ~2;
}

static inline uint64_t get_left_entry(void *node) {
  return (*((uint64_t *)node) &  0x3fffffffff0) >> 4;
}

static inline void set_left_entry(void *node, uint64_t val) {
  *((uint64_t *)node) &= 0xfffffc000000000f;
  *((uint64_t *)node) ^= val << 4;
}

static inline uint64_t get_right_entry(void *node) {
  return (*((uint64_t *)(node + 2)) & 0xfffffffffc000000) >> 26;
}

static inline void set_right_entry(void *node, uint64_t val) {
  *((uint64_t *)(node + 2)) &= 0x3ffffff;
  *((uint64_t *)(node + 2)) ^= (val << 26);
}

static inline void * new_row(PyStemDBObject *stemdb) {

  uint64_t *num_free = stemdb->meta + META_NUM_FREE_ROWS_OFFSET;
  if (*num_free > 0) {
    (*num_free)--;
    return stemdb->data + stemdb->free[*num_free];
  }

  uint64_t *num_rows = stemdb->meta + META_NUM_ROWS_OFFSET;

  if (((*num_rows + 1) * stemdb->row_bytes) > stemdb->data_size) {
    madvise(stemdb->data + stemdb->data_size - ALLOCATION, ALLOCATION, MADV_RANDOM);
    stemdb->data_size += ALLOCATION;
    if (ftruncate(stemdb->data_fd, stemdb->data_size) == -1)
      return PyErr_Format(PyExc_OSError,"ftruncate failed");
    madvise(stemdb->data + stemdb->data_size - ALLOCATION, ALLOCATION, MADV_SEQUENTIAL);
  }

  (*num_rows)++;
  return stemdb->data + stemdb->row_bytes * (*num_rows - 1);

}

static inline void *new_node(stemdb_key *key) {
  uint64_t *num_free = key->meta + META_KEY_NUM_FREE_NODES_OFFSET;
  if (*num_free > 0) {
    (*num_free)--;
    return key->data + key->free[*num_free];
  }

  uint64_t *num_nodes = key->meta + META_KEY_NUM_NODES_OFFSET;
  if (((*num_nodes + 1) * NODE_SIZE) > key->data_size) {
    madvise(key->data + key->data_size - ALLOCATION, ALLOCATION, MADV_RANDOM);
    key->data_size += ALLOCATION;
    if (ftruncate(key->data_fd, key->data_size) == -1)
      return PyErr_Format(PyExc_OSError,"ftruncate failed");
    madvise(key->data + key->data_size - ALLOCATION, ALLOCATION, MADV_SEQUENTIAL);
  }

  (*num_nodes)++;
  return key->data + NODE_SIZE * (*num_nodes - 1);

}

static PyObject* stemdb_new(PyObject *self, PyObject *args) {

  PyObject *py_desc;
  char *dir_name = NULL, *file_name;

  if (!PyArg_ParseTuple(args, "O|s", &py_desc, &dir_name)) return NULL;

  if (!PySequence_Check(py_desc))
    return PyErr_Format(PyExc_TypeError,"first argument must support the sequence protocol");

  Py_ssize_t num_cols = PySequence_Length(py_desc), i;
  if (num_cols >= pow(2,16))
    return PyErr_Format(PyExc_TypeError,"too many columns");

  PyStemDBObject *stemdb;
  if ((stemdb = PyObject_New(PyStemDBObject,&PyStemDBType)) == NULL) return NULL;

  stemdb->meta_size = META_HEADER_SIZE+num_cols*sizeof(meta_col);
  stemdb->data_size = ALLOCATION;
  stemdb->free_size = ALLOCATION;
  stemdb->keys = NULL;
  stemdb->num_keys = 0;
  
  if (dir_name != NULL) {

    if (mkdir(dir_name, S_IRWXU | S_IRWXG) < 0)
      return PyErr_Format(PyExc_OSError,"could not create directory '%s'",dir_name);

    long dir_name_len = strlen(dir_name);
    file_name = malloc(dir_name_len + 6);
    strcpy(file_name,dir_name);

    strcpy(file_name+dir_name_len,"/data");
    stemdb->data_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    strcpy(file_name+dir_name_len,"/free");
    stemdb->free_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    strcpy(file_name+dir_name_len,"/meta");
    stemdb->meta_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if (stemdb->data_fd == -1 || stemdb->free_fd == -1 || stemdb->meta_fd == -1 || 
	ftruncate(stemdb->data_fd, stemdb->data_size) == -1 ||
	ftruncate(stemdb->free_fd, stemdb->free_size) == -1 ||
	ftruncate(stemdb->meta_fd, stemdb->meta_size) == -1)
      return PyErr_Format(PyExc_OSError,"failed to write files to disk");

    stemdb->dir_name = malloc(dir_name_len+1);
    strcpy(stemdb->dir_name,dir_name);
  } else {
    stemdb->dir_name = NULL;
    return Py_BuildValue("");
  }



  if ((stemdb->data = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->data_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap data segment: %d", errno);
  madvise(stemdb->data, stemdb->data_size, MADV_SEQUENTIAL);

  if ((stemdb->free = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->free_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap free segment: %d", errno);
  madvise(stemdb->free, stemdb->free_size, MADV_SEQUENTIAL);

  if ((stemdb->meta = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->meta_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap meta segment: %d", errno);
  madvise(stemdb->meta, stemdb->meta_size, MADV_SEQUENTIAL);
 
  stemdb->num_cols = num_cols;
  memcpy(stemdb->meta + META_NUM_COLS_OFFSET,&(stemdb->num_cols),2);
 
  stemdb->row_size = 0;

  for (i = 0; i < num_cols; i++) {

    PyObject *py_col = PySequence_GetItem(py_desc,i);
    if (!PySequence_Check(py_col))
      return PyErr_Format(PyExc_TypeError,"element at index %zd must support the sequence protocol",i);
    if (PySequence_Length(py_col) != 3)
      return PyErr_Format(PyExc_TypeError,"element at index %zd must have a length of 3",i);
 
    PyObject *py_col_name = PySequence_GetItem(py_col,0);
    if (!PyString_Check(py_col_name))
      return PyErr_Format(PyExc_TypeError,"element at index %zd must have its first subelement be a string",i);
    if (PyString_Size(py_col_name) > 32)
      return PyErr_Format(PyExc_TypeError,"element at index %zd must have its first subelement be a string up to 32 characters",i);
    char *col_name = PyString_AsString(py_col_name);
    
    PyObject *py_col_type = PySequence_GetItem(py_col,1);
    if (PyString_Size(py_col_type) != 1)
      return PyErr_Format(PyExc_TypeError,"element at index %zd must have a single character for column type",i);
    char *col_type = PyString_AsString(py_col_type);
    
    PyObject *py_num_bits = PySequence_GetItem(py_col,2);
    long num_bits = PyInt_AsLong(py_num_bits);
    if (num_bits < 0 || num_bits >= pow(2,16))
      return PyErr_Format(PyExc_TypeError,"element at index %zd must have a valid number of bits",i);
    if (!(col_type[0] == 'i' || col_type[0] == 'u' || col_type[0] == 'f') && (num_bits % 8 != 0))
      return PyErr_Format(PyExc_TypeError,"binary column at index %zd must have a number of bits divisible by 8",i);
    
    meta_col col;
    strcpy(col.name,col_name);
    col.type = col_type[0];
    col.num_bits = num_bits;
    col.bit_offset = stemdb->row_size;
    memcpy(stemdb->meta + META_HEADER_SIZE + i * sizeof(meta_col), &col, sizeof(meta_col));

    stemdb->row_size += num_bits;

  }

  stemdb->row_bytes = stemdb->row_size / 8;
  if (stemdb->row_bytes * 8 < stemdb->row_size)
    stemdb->row_bytes++;
  
  memcpy(stemdb->meta + META_ROW_SIZE_OFFSET,&(stemdb->row_size),4);
  uint64_t num_rows = 0,num_free_rows = 0;
  memcpy(stemdb->meta + META_NUM_ROWS_OFFSET,&(num_rows),8);
  memcpy(stemdb->meta + META_NUM_FREE_ROWS_OFFSET,&(num_free_rows),8); 

  return (PyObject *)stemdb;

}

stemdb_key* open_key(PyStemDBObject *stemdb, char *file_name) {
  
  uint16_t i;
  stemdb_key *key;
  char key_name[32] = "";
  strncat(key_name,file_name,strrchr(file_name,'.') - file_name);
  
  for (i = 0; i < stemdb->num_keys; i++) {
    key = stemdb->keys[i];
    if (strcmp(key->name,key_name) == 0)
      return key;
  }

  key = malloc(sizeof(stemdb_key));
  strcpy(key->name,key_name);

  stemdb->num_keys++;
  if (stemdb->keys == NULL)
    stemdb->keys = malloc(sizeof(stemdb_key *) * stemdb->num_keys);
  else
    stemdb->keys = realloc(stemdb->keys, sizeof(stemdb_key *) * stemdb->num_keys);
  stemdb->keys[stemdb->num_keys - 1] = key;

  return key;

}

static PyObject* stemdb_open(PyObject *self, PyObject *py_dir_name) {

  char *dir_name = PyString_AsString(py_dir_name);
  if (dir_name == NULL) return NULL;
  
  DIR *dp = opendir(dir_name);
  if (dp == NULL) PyErr_Format(PyExc_OSError,"could not open directory '%s'", dir_name);
  char * file_name = malloc(strlen(dir_name) + 50);
  char * fname_offset = file_name + strlen(dir_name) + 1;
  strcpy(file_name,dir_name);
  strcpy(fname_offset - 1,"/");
 

  PyStemDBObject *stemdb;
  stemdb_key *key;
  if ((stemdb = PyObject_New(PyStemDBObject,&PyStemDBType)) == NULL) return NULL;
  stemdb->keys = NULL;
  stemdb->num_keys = 0;

  struct dirent *ep;
  struct stat st;
  int fd;
  while ((ep = readdir(dp)) != NULL) {
    stat(ep->d_name,&st);
    strcpy(fname_offset, ep->d_name);
    fd = open(file_name, O_RDWR | O_NOATIME);
    if (strcmp(ep->d_name,"data") == 0) {
      stemdb->data_size = st.st_size;
      stemdb->data_fd = fd;
    } else if (strcmp(ep->d_name,"free") == 0) {
      stemdb->free_size = st.st_size;
      stemdb->free_fd = fd;
    } else if (strcmp(ep->d_name,"meta") == 0) {
      stemdb->meta_size = st.st_size;
      stemdb->meta_fd = fd;
    } else if (strstr(ep->d_name,".keyd")) {
      key = open_key(stemdb, ep->d_name);
      key->data_size = st.st_size;
      key->data_fd = fd;
    } else if (strstr(ep->d_name,".keyf")) {
      key = open_key(stemdb, ep->d_name);
      key->free_size = st.st_size;
      key->free_fd = fd;
    } else if (strstr(ep->d_name,".keym")) {
      key = open_key(stemdb, ep->d_name);
      key->meta_size = st.st_size;
      key->meta_fd = fd;
    }
    
    if (stemdb->data_fd == -1 || stemdb->free_fd == -1 || stemdb->meta_fd == -1)
      return PyErr_Format(PyExc_OSError,"failed to open files from disk: %s", file_name);

  }
  closedir(dp);

  stemdb->dir_name = malloc(strlen(dir_name) + 1);
  strcpy(stemdb->dir_name,dir_name);

  if ((stemdb->data = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->data_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap data segment");
  madvise(stemdb->data, stemdb->data_size, MADV_RANDOM);

  if ((stemdb->free = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->free_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap free segment");
  madvise(stemdb->free, stemdb->free_size, MADV_RANDOM);

  if ((stemdb->meta = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,stemdb->meta_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap meta segment");
  madvise(stemdb->meta, stemdb->meta_size, MADV_RANDOM);
 
  stemdb->num_cols = *(uint16_t*)(stemdb->meta + META_NUM_COLS_OFFSET);
  stemdb->row_size = *(uint32_t*)(stemdb->meta + META_ROW_SIZE_OFFSET);
  stemdb->row_bytes = stemdb->row_size / 8;
  if (stemdb->row_bytes * 8 < stemdb->row_size)
    stemdb->row_bytes++;

  uint16_t i;
  for (i = 0; i < stemdb->num_keys; i++) {
    key = stemdb->keys[i];

    if (key->data_fd == -1 || key->free_fd == -1 || key->meta_fd == -1) 
      return PyErr_Format(PyExc_OSError,"failed to open index file: %s", key->name);
    if ((key->data = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->data_fd,0)) == MAP_FAILED)
      return PyErr_Format(PyExc_OSError,"failed to mmap data segment");
    madvise(key->data, key->data_size, MADV_RANDOM);
    if ((key->free = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->free_fd,0)) == MAP_FAILED)
      return PyErr_Format(PyExc_OSError,"failed to mmap free segment");
    madvise(key->free, key->free_size, MADV_RANDOM);
    if ((key->meta = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->meta_fd,0)) == MAP_FAILED)
      return PyErr_Format(PyExc_OSError,"failed to mmap meta segment");
    madvise(key->meta, key->meta_size, MADV_RANDOM);

    key->many = *(uint64_t*)(key->meta + META_KEY_MANY_OFFSET);
  }

  return (PyObject *)stemdb;

}

static PyObject * stemdb_insert(PyStemDBObject *stemdb, PyObject *py_rows) {
  PyObject *row_iter,*py_row,*py_val;
  Py_ssize_t ncols,r,c,bufsize,bufpos;
  int64_t ival;
  uint64_t uval;
  uint32_t bit_pos;
  uint16_t num_bits,b;
  unsigned char *buffer;

  row_iter = PyObject_GetIter(py_rows);
  if (row_iter == NULL) return NULL;
  r = 0;
  while ((py_row = PyIter_Next(row_iter)) != NULL) {
    
    ncols = stemdb->num_cols;

    BITFIELD(stemdb->row_size,bf);
    bit_pos = 0;

    meta_col *db_col = stemdb->meta + META_HEADER_SIZE;
    for (c = 0; c < ncols; c++) {
      py_val = PySequence_GetItem(py_row, c);
      if (py_val == NULL) return NULL;
      
      num_bits = db_col[c].num_bits;
      switch(db_col[c].type) {
      case 'i':

	if (PyInt_Check(py_val)) ival = PyInt_AS_LONG(py_val);
	else if (PyLong_Check(py_val)) ival = PyLong_AsLongLong(py_val);
	else return PyErr_Format(PyExc_TypeError,"could not convert value to signed integer in row %zd",r);
	if (ival >= pow(2,num_bits-1) || ival < -pow(2,num_bits-1))
	  return PyErr_Format(PyExc_TypeError,"signed integer out of range in row %zd",r);

	for (b = 0; b < num_bits; b++, bit_pos++)
	  if (ival & (1 << b)) setbit(bf,bit_pos);
	  else unsetbit(bf,bit_pos);

	break;

      case 'u':

	if (PyInt_Check(py_val)) uval = PyInt_AS_LONG(py_val);
	else if (PyLong_Check(py_val)) uval = PyLong_AsLongLong(py_val);
	else return PyErr_Format(PyExc_TypeError,"could not convert value to unsigned integer in row %zd", r);
	if (uval >= pow(2,num_bits))
	  return PyErr_Format(PyExc_TypeError,"unsigned integer out of range in row %zd",r);

	for (b = 0; b < num_bits; b++, bit_pos++)
	  if (uval & (1 << b)) setbit(bf,bit_pos);
	  else unsetbit(bf,bit_pos);

	break;

      case 'f':
	return NULL;

      default:
	PyObject_AsReadBuffer(py_val,(const void **)(&buffer),&bufsize);
	if (bufsize*8 != num_bits) return PyErr_Format(PyExc_TypeError,"buffer must have 1-byte elements and a total size of %d bits at row %zd", num_bits,r);
	for (bufpos = 0; bufpos < bufsize; bufpos++)
	  for (b = 0; b < 8; b++, bit_pos++)
	    if (buffer[bufpos] & (1 << b)) setbit(bf,bit_pos);
	    else unsetbit(bf,bit_pos);

	break;
	
      }

      Py_DECREF(py_val);
    }

    
    void *row = new_row(stemdb);
    if (row == NULL) return NULL;
    memcpy(row,bf,stemdb->row_bytes);

    r++;
    //if (r % 1000000 == 0)
    //  printf("%zd rows processed\n", r);
    
    Py_DECREF(py_row);
  }
  Py_DECREF(row_iter);

  return Py_BuildValue("");
}


#define CONTINUE 0
#define FOUND_LEFT 1
#define FOUND_RIGHT 2
#define FOUND_LL 3
#define NOT_FOUND 4

static inline int find_row(stemdb_key *key, unsigned char bit, uint32_t bit_pos, void **cur_node) {
  uint64_t entry;
  if (is_linked_list(*cur_node))
    return FOUND_LL;
  
  if (bit) {
    if (right_is_leaf_node(*cur_node)) 
      return FOUND_RIGHT;
    entry = get_right_entry(*cur_node);
  } else {
    if (left_is_leaf_node(*cur_node)) 
      return FOUND_LEFT;
    entry = get_left_entry(*cur_node);
  }
  if (entry == 0) 
    return NOT_FOUND;
  *cur_node = key->data + entry;
  return CONTINUE;
  
}

void collect_children(stemdb_key *key, void *node, PyObject **collector) {
  uint64_t entry;
  if (is_linked_list(node)) {
    Py_ssize_t cur_size = PyTuple_Size(*collector);
    _PyTuple_Resize(collector,cur_size + 1);
    PyTuple_SetItem(*collector,cur_size,PyLong_FromLongLong(get_left_entry(node)));
    entry = get_right_entry(node);
    if (entry == 0) return;
    collect_children(key, key->data + entry, collector);
    return;
  }

  if (left_is_leaf_node(node)) {
    Py_ssize_t cur_size = PyTuple_Size(*collector);
    _PyTuple_Resize(collector,cur_size + 1);
    PyTuple_SetItem(*collector,cur_size,PyLong_FromLongLong(get_left_entry(node)));
  } else {
    entry = get_left_entry(node);
    if (entry != 0)
      collect_children(key, key->data + entry, collector);
  }
  
  if (right_is_leaf_node(node)) {
    Py_ssize_t cur_size = PyTuple_Size(*collector);
    _PyTuple_Resize(collector,cur_size + 1);
    PyTuple_SetItem(*collector,cur_size,PyLong_FromLongLong(get_right_entry(node)));
  } else {
    entry = get_right_entry(node);
    if (entry != 0)
      collect_children(key, key->data + entry, collector);
  }

}

static PyObject * stemdb_get(PyStemDBObject *stemdb, PyObject *args) {

  char *key_name;
  PyObject *py_vals, *py_val;
  if (!PyArg_ParseTuple(args, "sO", &key_name, &py_vals)) return NULL;

  Py_ssize_t i,c,b,bufsize,bufpos;
  stemdb_key *key;
  int64_t ival;
  uint64_t uval;
  unsigned char *buffer;

  int found = 0;
  for (i = 0; i < stemdb->num_keys; i++) {
    key = stemdb->keys[i];
    if (strcmp(key->name,key_name) == 0) {
      found = 1;
      break;
    }
  }
  if (!found)
    return PyErr_Format(PyExc_ValueError,"key not found");
  
  uint16_t *num_key_cols = key->meta + META_KEY_NUM_COLS_OFFSET, num_val_cols;
  if (!PySequence_Check(py_vals))
    return PyErr_Format(PyExc_TypeError,"second argument must support the sequence protocol");
  num_val_cols = PySequence_Length(py_vals);
  if (num_val_cols > *num_key_cols)
    return PyErr_Format(PyExc_TypeError,"too many columns");

  uint16_t *col_indexes = key->meta + META_KEY_COLS_OFFSET;
  meta_col *db_col = stemdb->meta + META_HEADER_SIZE;
  uint32_t bit_pos = 0, num_bits;
  void *cur_node = key->data;

  found = NOT_FOUND;

  for (i = 0; i < num_val_cols; i++) {  
    py_val = PySequence_GetItem(py_vals,i);
    c = col_indexes[i];
    num_bits = db_col[c].num_bits;

    switch(db_col[c].type) {
    case 'i':

      if (PyInt_Check(py_val)) ival = PyInt_AS_LONG(py_val);
      else if (PyLong_Check(py_val)) ival = PyLong_AsLongLong(py_val);
      else return PyErr_Format(PyExc_TypeError,"could not convert value to signed integer in column %zd",i);
      if (ival >= pow(2,num_bits-1) || ival < -pow(2,num_bits-1))
	return PyErr_Format(PyExc_TypeError,"signed integer out of range in column %zd",i);
      
      for (b = 0; b < num_bits; b++, bit_pos++) {
	if (ival & (1 << b)) found = find_row(key, 1, bit_pos, &cur_node);
	else found = find_row(key, 0, bit_pos, &cur_node);
	if (found != CONTINUE) break;
      }
      
      break;
      
    case 'u':
      
      if (PyInt_Check(py_val)) uval = PyInt_AS_LONG(py_val);
      else if (PyLong_Check(py_val)) uval = PyLong_AsLongLong(py_val);
      else return PyErr_Format(PyExc_TypeError,"could not convert value to unsigned integer in column %zd", i);
      if (uval >= pow(2,num_bits))
	return PyErr_Format(PyExc_TypeError,"unsigned integer out of range in column %zd",i);
      
      for (b = 0; b < num_bits; b++, bit_pos++) {
	if (uval & (1 << b)) found = find_row(key, 1, bit_pos, &cur_node);
	else found = find_row(key, 0, bit_pos, &cur_node);
	if (found != CONTINUE) break;
      }

      break;
      
    case 'f':
      return NULL;
      
    default:
      PyObject_AsReadBuffer(py_val,(const void **)(&buffer),&bufsize);
      if (bufsize*8 != num_bits) return PyErr_Format(PyExc_TypeError,"buffer must have 1-byte elements and a total size of %d bits at column %zd", num_bits,i);
      for (bufpos = 0; bufpos < bufsize; bufpos++)
	for (b = 0; b < 8; b++, bit_pos++) {
	  if (buffer[bufpos] & (1 << b)) found = find_row(key, 1, bit_pos, &cur_node);
	  else found = find_row(key, 0, bit_pos, &cur_node);
	  if (found != CONTINUE) break;
	}
      break;
      
    }
    if (found != CONTINUE) break;
  }

  PyObject *py_indexes = PyTuple_New(0);
  if (found == NOT_FOUND) 
    return py_indexes;

  if (found == CONTINUE || found == FOUND_LL) {
    collect_children(key,cur_node,&py_indexes);
    return py_indexes;
  }

  uint64_t entry;
  if (found == FOUND_LEFT) entry = get_left_entry(cur_node);
  else entry = get_right_entry(cur_node);

  unsigned char *row = stemdb->data + stemdb->row_bytes * entry;
    
  for (i = 0; i < num_val_cols; i++) {  
    py_val = PySequence_GetItem(py_vals,i);
    c = col_indexes[i];
    num_bits = db_col[c].num_bits;
    switch(db_col[c].type) {
    case 'i':

      if (PyInt_Check(py_val)) ival = PyInt_AS_LONG(py_val);
      else if (PyLong_Check(py_val)) ival = PyLong_AsLongLong(py_val);
      else return PyErr_Format(PyExc_TypeError,"could not convert value to signed integer in col %zd",i);
      if (ival >= pow(2,num_bits-1) || ival < -pow(2,num_bits-1))
	return PyErr_Format(PyExc_TypeError,"signed integer out of range in col %zd",i);
      
      for (b = 0; b < num_bits; b++, bit_pos++) {
	if (ival & (1 << b)) {
	  if (!isbitset(row,db_col[c].bit_offset+b)) 
	    return py_indexes;
	} else {
	  if (isbitset(row,db_col[c].bit_offset+b)) 
	    return py_indexes;
	}
      }
      
      break;
      
    case 'u':
      
      if (PyInt_Check(py_val)) uval = PyInt_AS_LONG(py_val);
      else if (PyLong_Check(py_val)) uval = PyLong_AsLongLong(py_val);
      else return PyErr_Format(PyExc_TypeError,"could not convert value to unsigned integer in col %zd", i);
      if (uval >= pow(2,num_bits))
	return PyErr_Format(PyExc_TypeError,"unsigned integer out of range in col %zd",i);
      
      for (b = 0; b < num_bits; b++, bit_pos++) {
	if (uval & (1 << b)) {
	  if (!isbitset(row,db_col[c].bit_offset+b))
	    return py_indexes;
	} else {
	  if (isbitset(row,db_col[c].bit_offset+b))
            return py_indexes;
	}
      }

      break;
      
    case 'f':
      return NULL;
      
    default:
      PyObject_AsReadBuffer(py_val,(const void **)(&buffer),&bufsize);
      if (bufsize*8 != num_bits) return PyErr_Format(PyExc_TypeError,"buffer must have 1-byte elements and a total size of %d bits at col %zd", num_bits,i);
      bit_pos = 0;
      for (bufpos = 0; bufpos < bufsize; bufpos++)
	for (b = 0; b < 8; b++, bit_pos++) {
	  if (buffer[bufpos] & (1 << b)) {
	    if (!isbitset(row,db_col[c].bit_offset+bit_pos))
	      return py_indexes;
	  } else {
	    if (isbitset(row,db_col[c].bit_offset+bit_pos))
	      return py_indexes;
	  }
	}
      break;
      
    }
  }

  _PyTuple_Resize(&py_indexes,1);
  PyTuple_SetItem(py_indexes,0,PyLong_FromLongLong(entry));

  return py_indexes;
}

void * index_row(PyStemDBObject *stemdb, uint64_t index, stemdb_key *key) {

  uint16_t i,b, *num_key_cols = key->meta + META_KEY_NUM_COLS_OFFSET;
  uint32_t bit_pos = 0;
  uint16_t *col_indexes = key->meta + META_KEY_COLS_OFFSET;
  void *cur_node = key->data, *next_node, *first_node;
  meta_col *col;
  uint64_t entry,carry_entry = 0, many;
  unsigned char bit, *row = stemdb->data + stemdb->row_bytes * index;

  for (i = 0; i < *num_key_cols; i++) {
    col = stemdb->meta + META_HEADER_SIZE + col_indexes[i] * sizeof(meta_col);
    for (b = 0; b < col->num_bits; b++, bit_pos++) {

      if (is_linked_list(cur_node)) {
	first_node = cur_node;
	many = 1;
	entry = get_right_entry(cur_node);
	while (entry != 0) { 
	  many += 1;
	  cur_node = key->data + entry;
	  entry = get_right_entry(cur_node);
	}

	if (many > key->many && key->many != 0)
	  return first_node;

	next_node = new_node(key);
	set_linked_list(new_node);
	set_left_entry(new_node,index);
	set_right_entry(new_node,0);
	set_right_entry(cur_node,next_node - key->data);
	return NULL;
      }


      bit = isbitset(row,col->bit_offset+b);
      if (bit) { // right
	entry = get_right_entry(cur_node);
	if (right_is_leaf_node(cur_node)) { // leaf node
	  if ((b == (col->num_bits - 1)) && (i == (*num_key_cols - 1)) && (key->many == 1))
	    return cur_node; // todo: return indication of which side is the dupe
	  next_node = new_node(key);
	  memset(next_node,0,NODE_SIZE);
	  carry_entry = entry;
	  right_unset_leaf_node(cur_node);
	  set_right_entry(cur_node,next_node - key->data);
	  cur_node = next_node;
	} else { // pointer node
	  if (entry == 0) { // null pointer
	    if ((!left_is_leaf_node(cur_node)) && (get_left_entry(cur_node) == 0) && (cur_node != key->data)) {
	      if (isbitset(stemdb->data + stemdb->row_bytes * carry_entry, col->bit_offset+b)) {
		next_node = new_node(key);
		memset(next_node,0,NODE_SIZE);
		right_unset_leaf_node(cur_node);
		set_right_entry(cur_node,next_node - key->data);
		cur_node = next_node;
	      } else { 
		set_left_entry(cur_node,carry_entry);
		left_set_leaf_node(cur_node);
		set_right_entry(cur_node, index);
		right_set_leaf_node(cur_node);

		return NULL;
	      }
	    } else {
	      set_right_entry(cur_node, index);
	      right_set_leaf_node(cur_node);
	      return NULL;
	    }
	  } else { // valid pointer
	    cur_node = key->data + entry;
	  }
	}
      } else { // left
	entry = get_left_entry(cur_node);
	if (left_is_leaf_node(cur_node)) { // leaf node
	  if ((b == (col->num_bits - 1)) && (i == (*num_key_cols - 1)) && (key->many == 1))
	    return cur_node; // todo: return indication of which side is the dupe

	  next_node = new_node(key);
	  memset(next_node,0,NODE_SIZE);

	  carry_entry = entry;
	  left_unset_leaf_node(cur_node);
	  set_left_entry(cur_node,next_node - key->data);
	  cur_node = next_node;
	  
	} else { // pointer node
	  if (entry == 0) { // null pointer
	    if ((!right_is_leaf_node(cur_node)) && (get_right_entry(cur_node) == 0) && (cur_node != key->data)) {
	      if (!isbitset(stemdb->data + stemdb->row_bytes * carry_entry, col->bit_offset+b)) {
		next_node = new_node(key);
		memset(next_node,0,NODE_SIZE);
		left_unset_leaf_node(cur_node);
		set_left_entry(cur_node,next_node - key->data);
		cur_node = next_node;
	      } else {
		set_right_entry(cur_node,carry_entry);
		right_set_leaf_node(cur_node);
		set_left_entry(cur_node, index);
		left_set_leaf_node(cur_node);
		return NULL;
	      }
	    } else {
	      set_left_entry(cur_node, index);
	      left_set_leaf_node(cur_node);
	      return NULL;
	    }
	  } else { // valid pointer
	    cur_node = key->data + entry;
	  }
	}
      }
    } 
  }


  set_left_entry(cur_node,carry_entry);
  set_linked_list(cur_node);

  return NULL;

}

// stemdbtype.add_key(name,[col1,col2,...],nuniques=None)
static PyObject * stemdb_add_key(PyStemDBObject *stemdb, PyObject *args) {

  PyObject *py_cols,*py_col_name;
  char *key_name,*col_name;
  Py_ssize_t many,num_key_cols,i,c;
  uint16_t *col_indexes;
  uint64_t many_packed;

  if (!PyArg_ParseTuple(args, "sO|n", &key_name, &py_cols, &many)) return NULL;

  many_packed = many;
  
  if (!PySequence_Check(py_cols))
    return PyErr_Format(PyExc_TypeError,"second argument must support the sequence protocol");
  
  num_key_cols = PySequence_Length(py_cols);
  if (num_key_cols > stemdb->num_cols)
    return PyErr_Format(PyExc_TypeError,"too many columns");
  
  long key_name_len = strlen(key_name);
  if (key_name_len > 32)
    return PyErr_Format(PyExc_TypeError,"key name must be max 32 characters");
      
  col_indexes = malloc(num_key_cols * sizeof(uint16_t));

  for (i = 0; i < num_key_cols; i++) {
    py_col_name = PySequence_GetItem(py_cols,i);
    col_name = PyString_AsString(py_col_name);
    if (col_name == NULL) return NULL;
    int found = 0;
    for (c = 0; c < stemdb->num_cols; c++) {
      meta_col *col = stemdb->meta + META_HEADER_SIZE + c * sizeof(meta_col);
      if (strcmp(col->name,col_name) == 0) {
	col_indexes[i] = c;
	found = 1;
	break;
      }
    }
    if (!found) 
      return PyErr_Format(PyExc_TypeError,"column name not found");
  }

  stemdb_key *key = malloc(sizeof(stemdb_key));
  strcpy(key->name,key_name);

  key->many = many;
  key->meta_size = META_KEY_COLS_OFFSET + num_key_cols * sizeof(uint16_t);
  key->data_size = ALLOCATION;
  key->free_size = ALLOCATION;

  if (stemdb->dir_name != NULL) {

    long dir_name_len = strlen(stemdb->dir_name);
    char *file_name = malloc(dir_name_len + 1 + key_name_len + 6);
    strcpy(file_name,stemdb->dir_name);
    file_name[dir_name_len] = '/';
    strcpy(file_name+dir_name_len + 1,key_name);
    
    strcpy(file_name+dir_name_len+1+key_name_len,".keyd");
    key->data_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    strcpy(file_name+dir_name_len+1+key_name_len,".keym");
    key->meta_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    strcpy(file_name+dir_name_len+1+key_name_len,".keyf");
    key->free_fd = open(file_name, O_RDWR | O_CREAT | O_TRUNC | O_NOATIME, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);

    if (key->data_fd == -1 || key->free_fd == -1 || key->meta_fd == -1 || 
	ftruncate(key->data_fd, key->data_size) == -1 ||
	ftruncate(key->free_fd, key->free_size) == -1 ||
	ftruncate(key->meta_fd, key->meta_size) == -1)
      return PyErr_Format(PyExc_OSError,"failed to write files to disk");
  } else {

    return PyErr_Format(PyExc_OSError,"In-memory indexing not implemented yet");
  }

  if ((key->data = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->data_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap data segment");
  madvise(key->data, key->data_size, MADV_SEQUENTIAL);

  if ((key->free = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->free_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap free segment");
  madvise(key->free, key->free_size, MADV_SEQUENTIAL);

  if ((key->meta = mmap(NULL,RESERVE,PROT_READ | PROT_WRITE,MAP_SHARED,key->meta_fd,0)) == MAP_FAILED)
    return PyErr_Format(PyExc_OSError,"failed to mmap meta segment");
  madvise(key->meta, key->meta_size, MADV_SEQUENTIAL);

  memcpy(key->meta + META_KEY_NUM_COLS_OFFSET,&(num_key_cols),sizeof(uint16_t));
  uint64_t num_nodes = 0,num_free_nodes = 0;
  memcpy(key->meta + META_KEY_NUM_NODES_OFFSET,&(num_nodes),8);
  memcpy(key->meta + META_KEY_NUM_FREE_NODES_OFFSET,&(num_free_nodes),8); 
  memcpy(key->meta + META_KEY_MANY_OFFSET, &(many_packed),8);
  memcpy(key->meta + META_KEY_COLS_OFFSET,col_indexes,sizeof(uint16_t)*num_key_cols);

  void *first_node = new_node(key);
  memset(first_node,0,NODE_SIZE);



  uint64_t *num_rows = stemdb->meta + META_NUM_ROWS_OFFSET;
  for (i = 0; i < *num_rows; i++)
    if (index_row(stemdb, i, key) != NULL)
      return PyErr_Format(PyExc_ValueError,"too many identical values found");


  stemdb->num_keys++;
  if (stemdb->keys == NULL)
    stemdb->keys = malloc(sizeof(stemdb_key *) * stemdb->num_keys);
  else
    stemdb->keys = realloc(stemdb->keys, sizeof(stemdb_key *) * stemdb->num_keys);

  stemdb->keys[stemdb->num_keys - 1] = key;

  
  return Py_BuildValue("");

}

static Py_ssize_t stemdb_length(PyStemDBObject *stemdb) {
  return stemdb->num_cols;
}

static PyObject * stemdb_item(PyStemDBObject *stemdb, Py_ssize_t index) {
  PyObject *py_val,*py_row;
  Py_ssize_t ncols,c,bufsize,bufpos;
  int64_t ival;
  uint64_t uval;
  uint16_t num_bits,bit_pos,b;
  unsigned char *buffer;

  uint64_t *num_rows = stemdb->meta + META_NUM_ROWS_OFFSET;
  if (index >= *num_rows)
    return PyErr_Format(PyExc_TypeError,"index too large, db only has %llu rows",*num_rows);
  unsigned char *bf = stemdb->data + stemdb->row_bytes * index;
  bit_pos = 0;

  ncols = stemdb->num_cols;
  py_row = PyTuple_New(ncols);
  for (c = 0; c < ncols; c++) {

    meta_col *db_col = stemdb->meta + META_HEADER_SIZE;
    num_bits = db_col[c].num_bits;
    switch(db_col[c].type) {
    case 'i':

      ival = 0;
      for (b = 0; b < num_bits; b++, bit_pos++)
	if (isbitset(bf,bit_pos)) ival |= (1 << b);
	else ival &= ~(1 << b);
      if (isbitset(bf,bit_pos-1))
	ival |= ~((1 << num_bits) - 1);
      py_val = PyLong_FromLong(ival);
      PyTuple_SetItem(py_row,c,py_val);
      break;
      
    case 'u':
      
      uval = 0;
      for (b = 0; b < num_bits; b++, bit_pos++)
	if (isbitset(bf,bit_pos)) uval |= (1 << b);
	else uval &= ~(1 << b);
      py_val = PyLong_FromLong(uval);
      PyTuple_SetItem(py_row,c,py_val);
      break;

    case 'f':
      return NULL;
      
    default:
      bufsize = num_bits / 8;
      buffer = malloc(bufsize);
      for (bufpos = 0; bufpos < bufsize; bufpos++)
	for (b = 0; b < 8; b++, bit_pos++)
	  if (isbitset(bf,bit_pos)) setbit(buffer+bufpos,b);
	  else unsetbit(buffer+bufpos,b);
      py_val = PyString_FromStringAndSize((char *)buffer,bufsize);
      PyTuple_SetItem(py_row,c,py_val);
      break;
	
    }
  }
    
  return py_row;
}



static void stemdb_dealloc(PyStemDBObject *stemdb) {

  if (stemdb->data_fd >= 0)
    close(stemdb->data_fd);
  if (stemdb->free_fd >= 0)
    close(stemdb->free_fd);
  if (stemdb->meta_fd >= 0)
    close(stemdb->meta_fd);

  msync(stemdb->data, stemdb->data_size, MS_SYNC);
  msync(stemdb->free, stemdb->free_size, MS_SYNC);
  msync(stemdb->meta, stemdb->meta_size, MS_SYNC);
  munmap(stemdb->data, stemdb->data_size);
  munmap(stemdb->free, stemdb->free_size);
  munmap(stemdb->meta, stemdb->meta_size);
  
  uint16_t n;
  stemdb_key *key;
  for (n = 0; n < stemdb->num_keys; n++) {
    key = stemdb->keys[n];
    if (key->data_fd >= 0)
      close(key->data_fd);
    if (key->free_fd >= 0)
      close(key->free_fd);
    if (key->meta_fd >= 0)
      close(key->meta_fd);

    msync(key->data, stemdb->data_size, MS_SYNC);
    msync(key->free, stemdb->free_size, MS_SYNC);
    msync(key->meta, stemdb->meta_size, MS_SYNC);
    munmap(key->data, key->data_size);
    munmap(key->free, key->free_size);
    munmap(key->meta, key->meta_size);

    free(key);
  }
  
   
  PyObject_Del(stemdb);

}

static PyObject * stemdb_test(PyObject *self, PyObject *nothing) {
  void *node = malloc(10);
  memset(node,0,10);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  set_right_entry(node, 20);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  set_left_entry(node, 20);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  set_linked_list(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  left_set_leaf_node(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  right_set_leaf_node(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  unset_linked_list(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  left_unset_leaf_node(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  right_unset_leaf_node(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  set_right_entry(node,66763);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));
  set_left_entry(node,66763);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));  
  set_left_entry(node, 274877906943);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));  
  set_right_entry(node, 274877906943);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));  
  right_set_leaf_node(node);
  left_set_leaf_node(node);  
  set_linked_list(node);
  printf("left: %llu, right: %llu, left leaf: %d, right leaf: %d, linked list: %d\n",get_left_entry(node),get_right_entry(node),left_is_leaf_node(node),right_is_leaf_node(node),is_linked_list(node));  

  unsigned char *ch = node;
  long i;
  for (i = 0; i < 10; i++) {
    printf("0x%02X ",*ch);
    ch += 1;
  }
  printf("\n\n");

  return Py_BuildValue("");
}

static PyMethodDef stemdb_methods[] = {
  {"insert", (PyCFunction) stemdb_insert, METH_O},
  {"add_key", (PyCFunction) stemdb_add_key, METH_VARARGS},
  {"get", (PyCFunction) stemdb_get, METH_VARARGS}, 
  {NULL, NULL}
};

static PyObject * stemdb_getattr(PyObject *stemdb, char *name) {
  return Py_FindMethod(stemdb_methods, stemdb, name);
}


static PySequenceMethods stemdb_as_sequence = {
  (lenfunc)stemdb_length,/* sq_length */
  0,/* sq_concat */
  0,/* sq_repeat */
  (ssizeargfunc)stemdb_item,/* sq_item */
  0,/* sq_slice */
  0,/* sq_ass_item */
  0,/* sq_ass_slice */
  0,/* sq_contains */
  0,/* sq_inplace_concat */
  0,/* sq_inplace_repeat */
};  

static PyTypeObject PyStemDBType = {
  PyObject_HEAD_INIT(NULL)
  0,
  "stemdb.stemdb",
  sizeof(PyStemDBObject),
  0,
  (destructor)stemdb_dealloc, /*tp_dealloc*/
  0,/*tp_print*/
  (getattrfunc)stemdb_getattr,/*tp_getattr*/
  0,/*tp_setattr*/
  0,/*tp_compare*/
  0,/*tp_repr*/
  0,/*tp_as_number*/
  &stemdb_as_sequence,/*tp_as_sequence*/
  0,/*tp_as_mapping*/
};


static PyMethodDef stemdb_module_methods[] = {
  {"new", stemdb_new, METH_VARARGS, "Create a new stemdb"},
  {"open", stemdb_open, METH_O, "Open a stemdb"},
  {"test", (PyCFunction) stemdb_test, METH_NOARGS},
  {NULL, NULL, 0, NULL}
};


PyMODINIT_FUNC initstemdb(void) {
  Py_InitModule("stemdb",stemdb_module_methods);
}
