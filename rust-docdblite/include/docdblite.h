#ifndef DOCDBLITE_H
#define DOCDBLITE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

// Error codes
typedef enum {
    DOCDBLITE_SUCCESS = 0,
    DOCDBLITE_NULL_POINTER = 1,
    DOCDBLITE_INVALID_UTF8 = 2,
    DOCDBLITE_JSON_ERROR = 3,
    DOCDBLITE_DATABASE_ERROR = 4,
    DOCDBLITE_DOCUMENT_NOT_FOUND = 5,
    DOCDBLITE_COLLECTION_NOT_FOUND = 6,
    DOCDBLITE_INVALID_FILTER = 7,
    DOCDBLITE_GENERAL_ERROR = 8
} DocDbErrorCode;

// Opaque handles
typedef struct DocDbHandle DocDbHandle;
typedef struct CollectionHandle CollectionHandle;

// Database operations
DocDbHandle* docdblite_new(void);
DocDbHandle* docdblite_new_with_dir(const char* dir_path);
void docdblite_free(DocDbHandle* handle);

// Collection operations
CollectionHandle* docdblite_add_collection(DocDbHandle* handle, const char* name);
void collection_free(CollectionHandle* handle);

// Document operations
DocDbErrorCode collection_insert_one(
    CollectionHandle* collection_handle,
    const char* json_doc,
    char** doc_id_out
);

DocDbErrorCode collection_find_one(
    CollectionHandle* collection_handle,
    const char* doc_id,
    char** json_out
);

DocDbErrorCode collection_count_documents(
    CollectionHandle* collection_handle,
    const char* filter_json,
    int64_t* count_out
);

DocDbErrorCode collection_delete_one(
    CollectionHandle* collection_handle,
    const char* filter_json
);

// Memory management
void docdblite_free_string(char* ptr);

#ifdef __cplusplus
}
#endif

#endif // DOCDBLITE_H