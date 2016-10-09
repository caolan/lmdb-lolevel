(module lmdb-lolevel

;; Exports

;; Various flags and return codes are also exported (e.g. MDB_NOSYNC),
;; see lmdb-defs.

(mdb-strerror
 mdb-version
 mdb-version-string
 mdb-env-create
 mdb-env-open
 mdb-env-copy
 mdb-env-copyfd
 ;mdb-env-copy2
 ;mdb-env-copyfd2
 ;mdb_env_stat
 ;mdb_env_info
 ;mdb_env_sync
 mdb-env-close
 mdb-txn-begin
 mdb-txn-commit
 mdb-txn-abort
 mdb-dbi-open
 mdb-dbi-close
 mdb-get
 mdb-put
 mdb-del)

(import chicken scheme foreign)
(use srfi-69 lolevel data-structures)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <errno.h>")


;; Record types

;; These give print nicely in the REPL and some type protection over
;; plain pointers. In my tests they also incur slightly less overhead
;; than tagged-pointers with a tagged-pointer? check.

(define-record mdb-env pointer)
(define-record-printer (mdb-env x port)
  (fprintf port "#<mdb-env ~S>" (mdb-env-pointer x)))

(define-record mdb-txn pointer)
(define-record-printer (mdb-txn x port)
  (fprintf port "#<mdb-txn ~S>" (mdb-txn-pointer x)))

(define-record mdb-dbi handle)
(define-record-printer (mdb-dbi x port)
  (fprintf port "#<mdb-dbi ~S>" (mdb-dbi-handle x)))


;; This macro creates a variable associated with each foreign integer
;; and a hash-map which maps the integer back to a symbol - useful for
;; constructing error conditions.

(define-syntax lmdb-defs
  (syntax-rules ()
    ((_ mapname name ...)
     (begin
       (export name ...)
       (define name (foreign-value name int)) ...
       (define mapname
	 (alist->hash-table (list (cons name (quote name)) ...)
			    hash: number-hash
			    test: fx=))))))

;; Environment Flags
(lmdb-defs environment-flags
 MDB_FIXEDMAP
 MDB_NOSUBDIR
 MDB_NOSYNC
 MDB_RDONLY
 MDB_NOMETASYNC
 MDB_WRITEMAP
 MDB_MAPASYNC
 MDB_NOTLS
 MDB_NOLOCK
 MDB_NORDAHEAD
 MDB_NOMEMINIT)

;; Return Codes
(lmdb-defs return-codes
 MDB_SUCCESS
 MDB_KEYEXIST
 MDB_NOTFOUND
 MDB_PAGE_NOTFOUND
 MDB_CORRUPTED
 MDB_PANIC
 MDB_VERSION_MISMATCH
 MDB_INVALID
 MDB_MAP_FULL
 MDB_DBS_FULL
 MDB_READERS_FULL
 MDB_TLS_FULL
 MDB_TXN_FULL
 MDB_CURSOR_FULL
 MDB_PAGE_FULL
 MDB_MAP_RESIZED
 MDB_INCOMPATIBLE
 MDB_BAD_RSLOT
 MDB_BAD_TXN
 MDB_BAD_VALSIZE
 MDB_BAD_DBI
 
 ;; The following are not LMDB specific (defined in errno.h), but are
 ;; added to the hash table anyway as they can be helpful in
 ;; conditions e.g. (exn lmdb ENOENT). These integer codes can still
 ;; be passed to mdb_strerror as it's a superset of strerror(3).
 
 ENOENT
 EACCES
 EAGAIN
 ENOMEM
 EINVAL
 ENOSPC
 EIO)


;; Wraps calls to the C API which may return an error code. If
;; anything other than MDB_SUCCESS is returned, a scheme (abort)
;; occurs with an appropriate lmdb condition.

(define-syntax check-return
  (syntax-rules ()
    ((_ location x)
     (let ((code x))
       (when (not (fx= code MDB_SUCCESS))
	 (abort (lmdb-condition location code)))))))

(define (lmdb-condition name code)
  (make-composite-condition
   (make-property-condition 'exn
			    'message (mdb-strerror code)
			    'location name)
   (make-property-condition 'lmdb)
   (make-property-condition (hash-table-ref/default return-codes code 'unknown))))


(define c-mdb_version
  (foreign-lambda c-string "mdb_version"
    (c-pointer int)
    (c-pointer int)
    (c-pointer int)))

(define (mdb-version)
  (let-location ((major int)
		 (minor int)
		 (patch int))
    (c-mdb_version (location major)
		   (location minor)
		   (location patch))
    (vector major minor patch)))

(define (mdb-version-string)
  (c-mdb_version #f #f #f))

(define mdb-strerror
  (foreign-lambda c-string "mdb_strerror" int))


;; Environment

(define c-mdb_env_create
  (foreign-lambda int "mdb_env_create"
    (c-pointer (c-pointer (struct MDB_env)))))

(define (mdb-env-create)
  (let-location ((p (c-pointer (struct MDB_env))))
    (check-return 'mdb-env-create (c-mdb_env_create (location p)))
    (make-mdb-env p)))

;; for mdb-env-open mode argument use bitwise-ior with perm/... values
;; from posix module:

;; perm/irusr perm/iwusr perm/ixusr
;; perm/irgrp perm/iwgrp perm/ixgrp
;; perm/iroth perm/iwoth perm/ixoth
;; perm/irwxu perm/irwxg perm/irwxo
;; perm/isvtx perm/isuid perm/isgid

(define c-mdb_env_open
  (foreign-lambda int "mdb_env_open"
    (c-pointer (struct MDB_env))
    (const c-string)
    unsigned-int
    int))

(define (mdb-env-open env path flags mode)
  (check-return 'mdb-env-open
		(c-mdb_env_open (mdb-env-pointer env) path flags mode)))

(define c-mdb_env_copy
  (foreign-lambda int "mdb_env_copy"
    (c-pointer (struct MDB_env))
    (const c-string)))

(define (mdb-env-copy env path)
  (check-return 'mdb-env-copy
		(c-mdb_env_copy (mdb-env-pointer env) path)))

(define c-mdb_env_copyfd
  (foreign-lambda int "mdb_env_copyfd"
    (c-pointer (struct MDB_env))
    int))

(define (mdb-env-copyfd env fd)
  (check-return 'mdb-env-copyfd
		(c-mdb_env_copyfd (mdb-env-pointer env) fd)))

(define c-mdb_env_close
  (foreign-lambda void "mdb_env_close"
    (c-pointer (struct MDB_env))))

(define (mdb-env-close env)
  (c-mdb_env_close (mdb-env-pointer env)))


;; Transaction

(define c-mdb_txn_begin
  (foreign-lambda int "mdb_txn_begin"
    (c-pointer (struct MDB_env))
    (c-pointer (struct MDB_txn))
    unsigned-int
    (c-pointer (c-pointer (struct MDB_txn)))))

(define (mdb-txn-begin env parent flags)
  (let-location ((p (c-pointer (struct MDB_txn))))
    (check-return
     'mdb-txn-begin
     (c-mdb_txn_begin (mdb-env-pointer env) (and parent parent) flags (location p)))
    (make-mdb-txn p)))

(define c-mdb_txn_commit
  (foreign-lambda int "mdb_txn_commit" (c-pointer (struct MDB_txn))))

(define (mdb-txn-commit txn)
  (check-return 'mdb-txn-commit (c-mdb_txn_commit (mdb-txn-pointer txn))))

(define c-mdb_txn_abort
  (foreign-lambda void "mdb_txn_abort" (c-pointer (struct MDB_txn))))

(define (mdb-txn-abort txn)
  (c-mdb_txn_abort (mdb-txn-pointer txn)))


;; Database

(define c-mdb_dbi_open
  (foreign-lambda int "mdb_dbi_open"
    (c-pointer (struct MDB_txn))
    (const c-string)
    unsigned-int
    (c-pointer unsigned-int)))

(define (mdb-dbi-open txn name flags)
  (let-location ((h unsigned-int))
    (check-return 'mdb-dbi-open (c-mdb_dbi_open (mdb-txn-pointer txn) name flags (location h)))
    (make-mdb-dbi h)))

(define c-mdb_dbi_close
  (foreign-lambda void "mdb_dbi_close"
    (c-pointer (struct MDB_env))
    unsigned-int))

(define (mdb-dbi-close env dbi)
  (c-mdb_dbi_close (mdb-env-pointer env) (mdb-dbi-handle dbi)))


;; Data

(define memcpy*
  (foreign-lambda* (c-pointer void)
      ((scheme-object dest)
       ((const (c-pointer void)) src)
       (size_t size))
    "C_return(memcpy(C_data_pointer(dest), src, size));"))
		  
(define c-mdb_get
  (foreign-lambda* int
      (((c-pointer (struct MDB_txn)) txn)
       (unsigned-int dbi)
       (scheme-object key)
       ((c-pointer c-pointer) val_data)
       ((c-pointer size_t) val_size))
    "MDB_val k, data;
     int ret;
     C_i_check_string(key);
     k.mv_data = C_data_pointer(key);
     k.mv_size = C_header_size(key);
     if ((ret = mdb_get(txn, dbi, &k, &data))) {
         C_return(ret);
     }
     else {
         *val_data = data.mv_data;
         *val_size = data.mv_size;
         C_return(0);
     }"))

(define (mdb-get txn dbi key)
  (let-location ((val_data c-pointer)
		 (val_size size_t))
    (check-return 'mdb-get
		  (c-mdb_get (mdb-txn-pointer txn)
			     (mdb-dbi-handle dbi)
			     key
			     (location val_data)
			     (location val_size)))
    (let ((data (make-string val_size)))
      (memcpy* data val_data val_size)
      data)))

(define c-mdb_put
  (foreign-lambda* int
      (((c-pointer (struct MDB_txn)) txn)
       (unsigned-int dbi)
       (scheme-object key)
       (scheme-object data)
       (unsigned-int flags))
    "MDB_val k, v;
     C_i_check_string(key);
     C_i_check_string(data);
     k.mv_size = C_header_size(key);
     k.mv_data = C_data_pointer(key);
     v.mv_size = C_header_size(data);
     v.mv_data = C_data_pointer(data);
     C_return(mdb_put(txn, dbi, &k, &v, flags));"))
  
(define (mdb-put txn dbi key data flags)
  (check-return 'mdb-put
		(c-mdb_put (mdb-txn-pointer txn)
			   (mdb-dbi-handle dbi)
			   key
			   data
			   flags)))

(define c-mdb_del
  (foreign-lambda* int
      (((c-pointer (struct MDB_txn)) txn)
       (unsigned-int dbi)
       (c-pointer key_data)
       (size_t key_size)
       (c-pointer val_data)
       (size_t val_size))
    "MDB_val key;
     key.mv_size = key_size;
     key.mv_data = key_data;
     if (val_data != NULL) {
         MDB_val val;
         val.mv_size = val_size;
         val.mv_data = val_data;
         C_return(mdb_del(txn, dbi, &key, &val));
     }
     else {
         C_return(mdb_del(txn, dbi, &key, NULL));
     }"))

(define (mdb-del txn dbi key #!optional data)
  (check-return 'mdb-del (c-mdb_del
			  (mdb-txn-pointer txn)
			  (mdb-dbi-handle dbi)
			  (location key)
			  (string-length key)
			  (and data (location data))
			  (if data (string-length data) 0))))
  
)
