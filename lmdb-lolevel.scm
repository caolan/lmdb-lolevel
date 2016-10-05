(module lmdb-lolevel

;; exports
(mdb-env-open
 mdb-env-close
 mdb-txn-begin
 mdb-txn-commit
 mdb-txn-abort
 mdb-dbi-open
 mdb-dbi-close
 mdb-get
 mdb-put
 lmdb-env-pointer
 lmdb-txn-pointer
 lmdb-dbi-handle)

(import chicken scheme foreign)
(use srfi-69 lolevel)

(foreign-declare "#include <lmdb.h>")
(foreign-declare "#include <errno.h>")


;; This macro creates a variable associated with each foreign integer
;; and a hash-map which maps the integer back to a symbol - useful for
;; constructing error conditions.

(define-syntax lmdb-defs
  (syntax-rules ()
    ((_ mapname name ...)
     (begin
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


;; Wrapping these LMDB pointers in records is not strictly necessary
;; but provides nicer REPL output and a little type protection.

;; TODO: use tagged pointer instead??
;;       see lolevel tag-pointer, tagged-pointer? etc.
(define-record lmdb-env pointer)
(define-record-printer (lmdb-env x port)
  (fprintf port "#<lmdb-env ~S>" (lmdb-env-pointer x)))

(define-record lmdb-txn pointer)
(define-record-printer (lmdb-txn x port)
  (fprintf port "#<lmdb-txn ~S>" (lmdb-txn-pointer x)))

(define-record lmdb-dbi handle)
(define-record-printer (lmdb-dbi x port)
  (fprintf port "#<lmdb-dbi ~S>" (lmdb-dbi-handle x)))


;; Wraps calls to the C API which may return an error code. If
;; anything other than MDB_SUCCESS is returned, a scheme (abort)
;; occurs with an appropriate lmdb condition.

(define-syntax check-return
  (syntax-rules ()
    ((_ location (name args ...))
     (let ((code (name args ...)))
       (when (not (fx= code MDB_SUCCESS))
	 (abort (lmdb-condition location code)))))))

(define (lmdb-condition name code)
  (make-composite-condition
   (make-property-condition 'exn
			    'message (mdb-strerror code)
			    'location name)
   (make-property-condition 'lmdb)
   (make-property-condition (hash-table-ref return-codes code))))


(define mdb-strerror
  (foreign-lambda c-string "mdb_strerror" int))


;; Environment

(define c-mdb_env_create
  (foreign-lambda int "mdb_env_create"
    (c-pointer (c-pointer (struct MDB_env)))))

(define (mdb-env-create)
  (let-location ((p (c-pointer (struct MDB_env))))
    (check-return 'mdb-env-create (c-mdb_env_create (location p)))
    (make-lmdb-env p)))

(define c-mdb_env_open
  (foreign-lambda int "mdb_env_open"
    (c-pointer (struct MDB_env))
    (const c-string)
    unsigned-int
    int))

;; for mdb-env-open mode argument use bitwise-ior with perm/... values
;; from posix module:

;; perm/irusr perm/iwusr perm/ixusr
;; perm/irgrp perm/iwgrp perm/ixgrp
;; perm/iroth perm/iwoth perm/ixoth
;; perm/irwxu perm/irwxg perm/irwxo
;; perm/isvtx perm/isuid perm/isgid

(define (mdb-env-open path flags mode)
  (let ((env (mdb-env-create)))
    (check-return 'mdb-env-open
		  (c-mdb_env_open
		   (lmdb-env-pointer env)
		   path
		   flags
		   mode))
    env))

(define c-mdb_env_close
  (foreign-lambda void "mdb_env_close"
    (c-pointer (struct MDB_env))))

(define (mdb-env-close env)
  (c-mdb_env_close (lmdb-env-pointer env))
  (lmdb-env-pointer-set! env #f))


;; Transaction

(define c-mdb_txn_begin
  (foreign-lambda int "mdb_txn_begin"
    (c-pointer (struct MDB_env))
    (c-pointer (struct MDB_txn))
    unsigned-int
    (c-pointer (c-pointer (struct MDB_txn)))))

(define (mdb-txn-begin env parent flags)
  (let-location ((p (c-pointer (struct MDB_txn))))
    (check-return 'mdb-txn-begin
		  (c-mdb_txn_begin (lmdb-env-pointer env)
				   (and parent (lmdb-txn-pointer parent))
				   flags
				   (location p)))
    (make-lmdb-txn p)))

(define c-mdb_txn_commit
  (foreign-lambda int "mdb_txn_commit" (c-pointer (struct MDB_txn))))

(define (mdb-txn-commit txn)
  (check-return 'mdb-txn-commit (c-mdb_txn_commit (lmdb-txn-pointer txn))))

(define c-mdb_txn_abort
  (foreign-lambda void "mdb_txn_abort" (c-pointer (struct MDB_txn))))

(define (mdb-txn-abort txn)
  (c-mdb_txn_abort (lmdb-txn-pointer txn)))


;; Database

(define c-mdb_dbi_open
  (foreign-lambda int "mdb_dbi_open"
    (c-pointer (struct MDB_txn))
    (const c-string)
    unsigned-int
    (c-pointer unsigned-int)))

(define (mdb-dbi-open txn name flags)
  (let-location ((h unsigned-int))
    (check-return 'mdb-dbi-open
		  (c-mdb_dbi_open (lmdb-txn-pointer txn)
				  name
				  flags
				  (location h)))
    (make-lmdb-dbi h)))

(define c-mdb_dbi_close
  (foreign-lambda void "mdb_dbi_close"
    (c-pointer (struct MDB_env))
    unsigned-int))

(define (mdb-dbi-close env dbi)
  (c-mdb_dbi_close (lmdb-env-pointer env)
		   (lmdb-dbi-handle dbi)))


;; Data

(define c-mdb_get
  (foreign-lambda* int
    (((c-pointer (struct MDB_txn)) txn)
     (unsigned-int dbi)
     (c-pointer key_data)
     (size_t key_size)
     ((c-pointer c-pointer) val_data)
     ((c-pointer size_t) val_size))
    "MDB_val key, data;
     int ret;
     key.mv_data = key_data;
     key.mv_size = key_size;
     if ((ret = mdb_get(txn, dbi, &key, &data))) {
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
		  (c-mdb_get
		   (lmdb-txn-pointer txn)
		   (lmdb-dbi-handle dbi)
		   (location key)
		   (string-length key)
		   (location val_data)
		   (location val_size)))
    (let ((data (make-string val_size)))
      (move-memory! val_data data val_size)
      data)))

(define c-mdb_put
  (foreign-lambda* int
    (((c-pointer (struct MDB_txn)) txn)
     (unsigned-int dbi)
     (c-pointer key_data)
     (size_t key_size)
     (c-pointer val_data)
     (size_t val_size)
     (unsigned-int flags))
    "MDB_val key, val;
     key.mv_size = key_size;
     key.mv_data = key_data;
     val.mv_size = val_size;
     val.mv_data = val_data;
     C_return(mdb_put(txn, dbi, &key, &val, flags));"))

(define (mdb-put txn dbi key data flags)
  (check-return 'mdb-put
		(c-mdb_put (lmdb-txn-pointer txn)
			   (lmdb-dbi-handle dbi)
			   (location key)
			   (string-length key)
			   (location data)
			   (string-length data)
			   flags)))
     
)
