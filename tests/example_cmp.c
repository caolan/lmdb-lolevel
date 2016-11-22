#include "example_cmp.h"

int example_cmp(const MDB_val *a, const MDB_val *b) {
  char ca = ((char *)a->mv_data)[1];
  char cb = ((char *)b->mv_data)[1];
  if (ca > cb) {
    return 1;
  }
  else if (ca < cb) {
    return -1;
  }
  return 0;
}
