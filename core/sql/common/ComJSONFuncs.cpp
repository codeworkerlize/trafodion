/*-------------------------------------------------------------------------
 *
 * jsonfuncs.c
 *		Functions to process JSON data types.
 *
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/jsonfuncs.c
 *
 *-------------------------------------------------------------------------
 */
/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/

#include "ComJSONStringInfo.h"
#include "ComJSON.h"
#include <stdarg.h>
#include <limits.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include "str.h"

/* semantic action functions for json_get* functions */
static JsonReturnType get_object_start(void *state, JsonTokenType toktype);
static JsonReturnType get_object_end(void *state, JsonTokenType toktype);
static JsonReturnType get_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype);
static JsonReturnType get_object_field_end(void *state, char *fname, bool isnull, JsonTokenType toktype);
static JsonReturnType get_array_start(void *state, JsonTokenType toktype);
static JsonReturnType get_array_end(void *state, JsonTokenType toktype);
static JsonReturnType get_array_element_start(void *state, bool isnull, JsonTokenType toktype);
static JsonReturnType get_array_element_end(void *state, bool isnull, JsonTokenType toktype);
static JsonReturnType get_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for ansi json_value and json_query functions */
static JsonReturnType ansi_json_get_object_start(void *state, JsonTokenType toktype);
static JsonReturnType ansi_json_get_object_end(void *state, JsonTokenType toktype);
static JsonReturnType ansi_json_get_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype);
static JsonReturnType ansi_json_get_object_field_end(void *state, char *fname, bool isnull, JsonTokenType toktype);
static JsonReturnType ansi_json_get_array_start(void *state, JsonTokenType toktype);
static JsonReturnType ansi_json_get_array_end(void *state, JsonTokenType toktype);
static JsonReturnType ansi_json_get_array_element_start(void *state, bool isnull, JsonTokenType toktype);
static JsonReturnType ansi_json_get_array_element_end(void *state, bool isnull, JsonTokenType toktype);
static JsonReturnType ansi_json_get_scalar(void *state, char *token, JsonTokenType tokentype);


/* common worker function for json getter functions */
static JsonReturnType get_path_all(bool as_text, char *json, short nargs, va_list args, char **result);
static JsonReturnType get_worker(char *json, char **tpath, int *ipath, int npath,
                                 bool normalize_results, char **result);


/* semantic action functions for json_array_length */
static void alen_object_start(void *state, JsonTokenType toktype);
static void alen_scalar(void *state, char *token, JsonTokenType tokentype);
static void alen_array_element_start(void *state, bool isnull, JsonTokenType toktype);

/* common workers for json{b}_each* functions */

/* semantic action functions for json_each */
static void each_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype);
static void each_object_field_end(void *state, char *fname, bool isnull, JsonTokenType toktype);
static void each_array_start(void *state, JsonTokenType toktype);
static void each_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for json_array_elements */
static void elements_object_start(void *state, JsonTokenType toktype);
static void elements_array_element_start(void *state, bool isnull, JsonTokenType toktype);
static void elements_array_element_end(void *state, bool isnull, JsonTokenType toktype);
static void elements_scalar(void *state, char *token, JsonTokenType tokentype);

/* semantic action functions for json_strip_nulls */
static void sn_object_start(void *state, JsonTokenType toktype);
static void sn_object_end(void *state, JsonTokenType toktype);
static void sn_array_start(void *state, JsonTokenType toktype);
static void sn_array_end(void *state, JsonTokenType toktype);
static void sn_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype);
static void sn_array_element_start(void *state, bool isnull, JsonTokenType toktype);
static void sn_scalar(void *state, char *token, JsonTokenType tokentype);

static bool is_json_type_scalar(JsonTokenType type)
{
    bool ret = FALSE;

    switch(type)
    {
        case JSON_TOKEN_STRING:
        case JSON_TOKEN_NUMBER:
        case JSON_TOKEN_TRUE:
        case JSON_TOKEN_FALSE:
        case JSON_TOKEN_NULL:
            ret = TRUE;
            break;
        default:
            break;
    }

    return ret;
}

class JsonResultElem
{
public:

    JsonResultElem()
        : heap_(NULL), result_value_(NULL), is_scalar_(FALSE), type_(JSON_TOKEN_INVALID)
    {}

    JsonResultElem(CollHeap * heap, char * val, int val_len, bool is_scalar = FALSE, JsonTokenType type = JSON_TOKEN_INVALID)
        : heap_(heap), is_scalar_(is_scalar), type_(type)
    {
        if (val != NULL && val_len > 0)
        {
            result_value_ = new(heap_) char[val_len + 1];
            memcpy(result_value_, val, val_len);
            result_value_[val_len] = '\0';
        }
        else
            result_value_ = NULL;
    }

    ~JsonResultElem()
    {
        NADELETEBASIC(result_value_, heap_);
    }

    JsonResultElem & operator= (const JsonResultElem & src)
    {
        heap_ = src.heap_;
        is_scalar_ = src.is_scalar_;
        type_ = src.type_;

        if (src.result_value_ != NULL)
        {
            result_value_ = new(heap_) char[strlen(src.result_value_) + 1];
            strcpy(result_value_, src.result_value_);
        }
        else
            result_value_ = NULL;

        return *this;
    }

    bool is_scalar() {return is_scalar_;}
	bool is_array() {return JSON_TOKEN_ARRAY_END == type_ || JSON_TOKEN_ARRAY_START == type_;}

    int get_result_len() {return strlen(result_value_);}

    NAString get_result_str(bool need_quote = FALSE)
    {
        NAString result(result_value_, heap_);

        if (TRUE == need_quote &&
            TRUE == is_scalar_ && JSON_TOKEN_STRING == type_)
        {
            result.insert(result.length(), "\"");
            result.insert(0, "\"");
        }

        return result;
    }

private:
    bool           is_scalar_;
    JsonTokenType  type_;//only for scalar

    char          *result_value_;

    CollHeap      *heap_;
};


/* state for json_object_keys */
typedef struct OkeysState
{
    JsonLexContext *lex;
    char	  **result;
    int			result_size;
    int			result_count;
    int			sent_count;
} OkeysState;

/* state for json_get* functions */
typedef struct GetState
{
    JsonLexContext *lex;
    char	   *tresult;
    char	   *result_start;
    bool		normalize_results;
    bool		next_scalar;
    int			npath;			/* length of each path-related array */
    char	  **path_names;		/* field name(s) being sought */
    int		   *path_indexes;	/* array index(es) being sought */
    bool	   *pathok;			/* is path matched to current depth? */
    int		   *array_cur_index;	/* current element index at each path level */
} GetState;

/* state for json_array_length */
typedef struct AlenState
{
    JsonLexContext *lex;
    int			count;
} AlenState;


/* state for json_array_elements */
typedef struct ElementsState
{
    JsonLexContext *lex;
    const char *function_name;
    char	   *result_start;
    bool		normalize_results;
    bool		next_scalar;
    char	   *normalized_scalar;
} ElementsState;

/* state for json_strip_nulls */
typedef struct StripnullState
{
    JsonLexContext *lex;
    StringInfo	strval;
    bool		skip_next_null;
} StripnullState;


typedef struct JsonPathGetState
{
    JsonLexContext          *lex;

    NAList<JsonResultElem>   tresult_l;

    char                    *result_start;
    bool                     normalize_results;
    bool                     next_scalar;

    int                      path_level;
    bool                     path_lvl_a; /* JSON string start with arry */
    int                      npath;
    PathNode                *paths;
    bool                    *path_k_ok;            /* is path matched to current depth? */
    int                     *array_cur_index;   /* current element index at each path level */
    int                     *array_elem_count;/*  element count at each level*/

    CollHeap                *heap;

    JsonPathGetState(CollHeap * h)
        : heap(h), tresult_l(h), lex(NULL), result_start(NULL), next_scalar(FALSE),
        path_level(0), npath(0), paths(NULL), path_k_ok(NULL), 
        array_cur_index(NULL), array_elem_count(NULL), path_lvl_a(FALSE)
    {}

    JsonReturnType get_json_path_result_str(char * & result, bool is_scalar, 
                                                       bool single_only, bool is_lax, 
                                                       JsonWrapperBehav wrap);

    NABoolean nodeHasIndex(Int32 level)
    {
        return !(NULL == paths->getChild(level)->getIndex());
    }
} JsonPathGetState;

JsonReturnType
json_extract_path(char **result, char *json, short nargs, ...)
{
    JsonReturnType ret = JSON_OK;
    va_list args;
    va_start(args, nargs);
    ret = get_path_all(true, json, nargs, args, result);
    va_end(args);
    return ret;
}

JsonReturnType
json_extract_path_text(char **result, char *json, short nargs, ...)
{
    JsonReturnType ret = JSON_OK;
    va_list args;
    va_start(args, nargs);
    ret = get_path_all(true, json, nargs, args, result);
    va_end(args);
    return ret;
}

/*
 * common routine for extract_path functions
 */
static JsonReturnType
get_path_all(bool as_text, char *json, short nargs, va_list args, char **result)
{
    bool	   *pathnulls;
    char	  **tpath;
    int		   *ipath;
    int			i;
    JsonReturnType ret = JSON_OK;

    tpath = (char **)malloc(nargs * sizeof(char *));
    ipath = (int *)malloc(nargs * sizeof(int));

    for (i = 0; i < nargs; i++)
    {
        tpath[i] = va_arg(args, char *);

        /*
         * we have no idea at this stage what structure the document is so
         * just convert anything in the path that we can to an integer and set
         * all the other integers to INT_MIN which will never match.
         */
        if (*tpath[i] != '\0')
        {
            long		ind;
            char	   *endptr;

            errno = 0;
            ind = strtol(tpath[i], &endptr, 10);
            if (*endptr == '\0' && errno == 0 && ind <= INT_MAX && ind >= INT_MIN)
                ipath[i] = (int) ind;
            else
                ipath[i] = INT_MIN;
        }
        else
            ipath[i] = INT_MIN;
    }

    ret = get_worker(json, tpath, ipath, nargs, as_text, result);

    if (tpath != NULL)
        free(tpath);
    if (ipath != NULL)
        free(ipath);
    return ret;
}

JsonReturnType json_object_field_text(char *json, char *fieldName, char **result)
{
    return get_worker(json, &fieldName, NULL, 1, true, result);
}

static JsonReturnType
get_worker(char *json,
           char **tpath,
           int *ipath,
           int npath,
           bool normalize_results,
           char **result)
{
    JsonLexContext *lex = makeJsonLexContext(json, true);
    JsonSemAction *sem = (JsonSemAction *)malloc(sizeof(JsonSemAction));
    GetState   *state = (GetState *)malloc(sizeof(GetState));
    JsonReturnType ret;

    memset(sem, 0, sizeof(JsonSemAction));
    memset(state, 0, sizeof(GetState));

    if(npath < 0)
        return JSON_UNEXPECTED_ERROR;

    state->lex = lex;
    /* is it "_as_text" variant? */
    state->normalize_results = normalize_results;
    state->npath = npath;
    state->path_names = tpath;
    state->path_indexes = ipath;
    state->pathok = (bool *)malloc(sizeof(bool) * npath);
    state->array_cur_index = (int *)malloc(sizeof(int) * npath);

    if (npath > 0)
        state->pathok[0] = true;

    sem->semstate = (void *) state;

    /*
     * Not all variants need all the semantic routines. Only set the ones that
     * are actually needed for maximum efficiency.
     */
    sem->scalar = get_scalar;
    if (npath == 0)
    {
        sem->object_start = get_object_start;
        sem->object_end = get_object_end;
        sem->array_start = get_array_start;
        sem->array_end = get_array_end;
    }
    if (tpath != NULL)
    {
        sem->object_field_start = get_object_field_start;
        sem->object_field_end = get_object_field_end;
    }
    if (ipath != NULL)
    {
        sem->array_start = get_array_start;
        sem->array_element_start = get_array_element_start;
        sem->array_element_end = get_array_element_end;
    }

    ret = pg_parse_json(lex, sem);
    if (ret == JSON_OK)
        *result = state->tresult;
    else
        *result = NULL;

    if (lex != NULL)
    {
        if (lex->strval != NULL)
        {
            if (lex->strval->data != NULL)
                free(lex->strval->data);
            free(lex->strval);
        }
        free(lex);
    }
    if (sem != NULL)
        free(sem);
    if (state != NULL)
        free(state);
    return ret;
}

static JsonReturnType
get_object_start(void *state, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    int			lex_level = _state->lex->lex_level;

    if (lex_level == 0 && _state->npath == 0)
    {
        /*
         * Special case: we should match the entire object.  We only need this
         * at outermost level because at nested levels the match will have
         * been started by the outer field or array element callback.
         */
        _state->result_start = _state->lex->token_start;
    }
    return JSON_OK;
}

static JsonReturnType
get_object_end(void *state, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    int			lex_level = _state->lex->lex_level;

    if (lex_level == 0 && _state->npath == 0)
    {
        /* Special case: return the entire object */
        char	   *start = _state->result_start;
        int			len = _state->lex->prev_token_terminator - start;

        //_state->tresult = cstring_to_text_with_len(start, len);
        _state->tresult = (char *)malloc(len + 1);

        memcpy(_state->tresult, start, len);
        _state->tresult[len] = '\0';
    }
    return JSON_OK;
}

static JsonReturnType
get_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    bool		get_next = false;
    int			lex_level = _state->lex->lex_level;

    if (lex_level <= _state->npath &&
            _state->pathok[lex_level - 1] &&
            _state->path_names != NULL &&
            _state->path_names[lex_level - 1] != NULL &&
            strcmp(fname, _state->path_names[lex_level - 1]) == 0)
    {
        if (lex_level < _state->npath)
        {
            /* if not at end of path just mark path ok */
            _state->pathok[lex_level] = true;
        }
        else
        {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    if (get_next)
    {
        /* this object overrides any previous matching object */
        _state->tresult = NULL;
        _state->result_start = NULL;

        if (_state->normalize_results &&
                _state->lex->token_type == JSON_TOKEN_STRING)
        {
            /* for as_text variants, tell get_scalar to set it for us */
            _state->next_scalar = true;
        }
        else
        {
            /* for non-as_text variants, just note the json starting point */
            _state->result_start = _state->lex->token_start;
        }
    }
    return JSON_OK;
}

static JsonReturnType
get_object_field_end(void *state, char *fname, bool isnull, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    bool		get_last = false;
    int			lex_level = _state->lex->lex_level;

    /* same tests as in get_object_field_start */
    if (lex_level <= _state->npath &&
            _state->pathok[lex_level - 1] &&
            _state->path_names != NULL &&
            _state->path_names[lex_level - 1] != NULL &&
            strcmp(fname, _state->path_names[lex_level - 1]) == 0)
    {
        if (lex_level < _state->npath)
        {
            /* done with this field so reset pathok */
            _state->pathok[lex_level] = false;
        }
        else
        {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* for as_text scalar case, our work is already done */
    if (get_last && _state->result_start != NULL)
    {
        /*
         * make a text object from the string from the prevously noted json
         * start up to the end of the previous token (the lexer is by now
         * ahead of us on whatever came after what we're interested in).
         */
        if (isnull && _state->normalize_results)
            _state->tresult = (char *) NULL;
        else
        {
            char	   *start = _state->result_start;
            int			len = _state->lex->prev_token_terminator - start;

            //_state->tresult = cstring_to_text_with_len(start, len);

            _state->tresult = (char *)malloc(len + 1);

            memcpy(_state->tresult, start, len);
            _state->tresult[len] = '\0';
        }

        /* this should be unnecessary but let's do it for cleanliness: */
        _state->result_start = NULL;
    }
    return JSON_OK;
}

static JsonReturnType
get_array_start(void *state, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    int			lex_level = _state->lex->lex_level;

    if (lex_level < _state->npath)
    {
        /* Initialize counting of elements in this array */
        _state->array_cur_index[lex_level] = -1;

        /* INT_MIN value is reserved to represent invalid subscript */
        if (_state->path_indexes[lex_level] < 0 &&
                _state->path_indexes[lex_level] != INT_MIN)
        {
            /* Negative subscript -- convert to positive-wise subscript */
            int		nelements;
            JsonReturnType ret = json_count_array_elements(_state->lex, nelements);
            if (ret != JSON_OK)
                return ret;
            if (-_state->path_indexes[lex_level] <= nelements)
                _state->path_indexes[lex_level] += nelements;
        }
    }
    else if (lex_level == 0 && _state->npath == 0)
    {
        /*
         * Special case: we should match the entire array.  We only need this
         * at the outermost level because at nested levels the match will
         * have been started by the outer field or array element callback.
         */
        _state->result_start = _state->lex->token_start;
    }
    return JSON_OK;
}

static JsonReturnType
get_array_end(void *state, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    int			lex_level = _state->lex->lex_level;

    if (lex_level == 0 && _state->npath == 0)
    {
        /* Special case: return the entire array */
        char	   *start = _state->result_start;
        int			len = _state->lex->prev_token_terminator - start;

        //_state->tresult = cstring_to_text_with_len(start, len);

        _state->tresult = (char *)malloc(len + 1);

        memcpy(_state->tresult, start, len);
        _state->tresult[len] = '\0';
    }
    return JSON_OK;
}

static JsonReturnType
get_array_element_start(void *state, bool isnull, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    bool		get_next = false;
    int			lex_level = _state->lex->lex_level;

    /* Update array element counter */
    if (lex_level <= _state->npath)
        _state->array_cur_index[lex_level - 1]++;

    if (lex_level <= _state->npath &&
            _state->pathok[lex_level - 1] &&
            _state->path_indexes != NULL &&
            _state->array_cur_index[lex_level - 1] == _state->path_indexes[lex_level - 1])
    {
        if (lex_level < _state->npath)
        {
            /* if not at end of path just mark path ok */
            _state->pathok[lex_level] = true;
        }
        else
        {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    /* same logic as for objects */
    if (get_next)
    {
        _state->tresult = NULL;
        _state->result_start = NULL;

        if (_state->normalize_results &&
                _state->lex->token_type == JSON_TOKEN_STRING)
            _state->next_scalar = true;
        else
            _state->result_start = _state->lex->token_start;
    }
    return JSON_OK;
}

static JsonReturnType
get_array_element_end(void *state, bool isnull, JsonTokenType toktype)
{
    GetState   *_state = (GetState *) state;
    bool		get_last = false;
    int			lex_level = _state->lex->lex_level;

    /* same tests as in get_array_element_start */
    if (lex_level <= _state->npath &&
            _state->pathok[lex_level - 1] &&
            _state->path_indexes != NULL &&
            _state->array_cur_index[lex_level - 1] == _state->path_indexes[lex_level - 1])
    {
        if (lex_level < _state->npath)
        {
            /* done with this element so reset pathok */
            _state->pathok[lex_level] = false;
        }
        else
        {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* same logic as for objects */
    if (get_last && _state->result_start != NULL)
    {
        if (isnull && _state->normalize_results)
            _state->tresult = (char *) NULL;
        else
        {
            char	   *start = _state->result_start;
            int			len = _state->lex->prev_token_terminator - start;

            //_state->tresult = cstring_to_text_with_len(start, len);

            _state->tresult = (char *)malloc(len + 1);

            memcpy(_state->tresult, start, len);
            _state->tresult[len] = '\0';
        }

        _state->result_start = NULL;
    }
    return JSON_OK;
}

static JsonReturnType
get_scalar(void *state, char *token, JsonTokenType tokentype)
{
    GetState   *_state = (GetState *) state;
    int			lex_level = _state->lex->lex_level;

    /* Check for whole-object match */
    if (lex_level == 0 && _state->npath == 0)
    {
        if (_state->normalize_results && tokentype == JSON_TOKEN_STRING)
        {
            /* we want the de-escaped string */
            _state->next_scalar = true;
        }
        else if (_state->normalize_results && tokentype == JSON_TOKEN_NULL)
            _state->tresult = (char *) NULL;
        else
        {
            /*
             * This is a bit hokey: we will suppress whitespace after the
             * scalar token, but not whitespace before it.  Probably not worth
             * doing our own space-skipping to avoid that.
             */
            char	   *start = _state->lex->input;
            int			len = _state->lex->prev_token_terminator - start;

            //_state->tresult = cstring_to_text_with_len(start, len);

            _state->tresult = (char *)malloc(len + 1);

            memcpy(_state->tresult, start, len);
            _state->tresult[len] = '\0';
        }
    }

    if (_state->next_scalar)
    {
        /* a de-escaped text value is wanted, so supply it */
        //_state->tresult = cstring_to_text(token);
        //_state->tresult = token;
        int len = str_len(token);
        _state->tresult = (char *)malloc(len + 1);

        memcpy(_state->tresult, token, len);
        _state->tresult[len] = '\0';
        /* make sure the next call to get_scalar doesn't overwrite it */
        _state->next_scalar = false;
    }
    return JSON_OK;
}

/*
 * These next two checks ensure that the json is an array (since it can't be
 * a scalar or an object).
 */

static void
alen_object_start(void *state, JsonTokenType toktype)
{
    AlenState  *_state = (AlenState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0)
        return;
}

static void
alen_scalar(void *state, char *token, JsonTokenType tokentype)
{
    AlenState  *_state = (AlenState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0)
        return;
}

static void
alen_array_element_start(void *state, bool isnull, JsonTokenType toktype)
{
    AlenState  *_state = (AlenState *) state;

    /* just count up all the level 1 elements */
    if (_state->lex->lex_level == 1)
        _state->count++;
}


static void
elements_object_start(void *state, JsonTokenType toktype)
{
    ElementsState *_state = (ElementsState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0)
        return;
}

static void
elements_scalar(void *state, char *token, JsonTokenType tokentype)
{
    ElementsState *_state = (ElementsState *) state;

    /* json structure check */
    if (_state->lex->lex_level == 0)
        return;

    /* supply de-escaped value if required */
    if (_state->next_scalar)
        _state->normalized_scalar = token;
}


/*
 * Semantic actions for json_strip_nulls.
 *
 * Simply repeat the input on the output unless we encounter
 * a null object field. State for this is set when the field
 * is started and reset when the scalar action (which must be next)
 * is called.
 */

static void
sn_object_start(void *state, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    appendStringInfoCharMacro(_state->strval, '{');
}

static void
sn_object_end(void *state, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    appendStringInfoCharMacro(_state->strval, '}');
}

static void
sn_array_start(void *state, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    appendStringInfoCharMacro(_state->strval, '[');
}

static void
sn_array_end(void *state, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    appendStringInfoCharMacro(_state->strval, ']');
}

static void
sn_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    if (isnull)
    {
        /*
         * The next thing must be a scalar or isnull couldn't be true, so
         * there is no danger of this state being carried down into a nested
         * object or array. The flag will be reset in the scalar action.
         */
        _state->skip_next_null = true;
        return;
    }

    if (_state->strval->data[_state->strval->len - 1] != '{')
        appendStringInfoCharMacro(_state->strval, ',');

    /*
     * Unfortunately we don't have the quoted and escaped string any more, so
     * we have to re-escape it.
     */
    escape_json(_state->strval, fname);

    appendStringInfoCharMacro(_state->strval, ':');
}

static void
sn_array_element_start(void *state, bool isnull, JsonTokenType toktype)
{
    StripnullState *_state = (StripnullState *) state;

    if (_state->strval->data[_state->strval->len - 1] != '[')
        appendStringInfoCharMacro(_state->strval, ',');
}

static void
sn_scalar(void *state, char *token, JsonTokenType tokentype)
{
    StripnullState *_state = (StripnullState *) state;

    if (_state->skip_next_null)
    {
        _state->skip_next_null = false;
        return;
    }

    if (tokentype == JSON_TOKEN_STRING)
        escape_json(_state->strval, token);
    else
        appendStringInfoString(_state->strval, token);
}


JsonReturnType ansi_json_get_worker(char *json, PathNode * path_elem_n, bool normalize_results, char **result, CollHeap * heap,
                                        bool is_scalar = TRUE, bool single_only = TRUE, bool is_lax = TRUE, 
                                        JsonWrapperBehav wrap = JSON_WITHOUT_WRAPPER)
{
    JsonLexContext *lex = makeJsonLexContext(json, true);
    JsonSemAction *sem = new (heap) JsonSemAction();
    JsonPathGetState *state = new(heap) JsonPathGetState(heap);
    JsonReturnType ret;

    state->npath = path_elem_n == NULL ? 0 : path_elem_n->getNodeNum();
    if(state->npath < 1)
        return JSON_UNEXPECTED_ERROR;

    state->lex = lex;
    /* is it "_as_text" variant? */
    state->normalize_results = normalize_results;
    state->paths = path_elem_n;
    state->path_level = 0;
    state->path_k_ok = new(heap) bool[state->npath];
    state->array_cur_index = new(heap) int[state->npath];
    state->array_elem_count = new(heap) int[state->npath];
    state->heap = heap;

    state->path_k_ok[state->path_level] = true;

    sem->semstate = (void *) state;

    /*
     * Not all variants need all the semantic routines. Only set the ones that
     * are actually needed for maximum efficiency.
     */
    sem->scalar = ansi_json_get_scalar;
    sem->object_start = ansi_json_get_object_start;
    sem->object_end = ansi_json_get_object_end;
    sem->array_start = ansi_json_get_array_start;
    sem->array_end = ansi_json_get_array_end;
    sem->object_field_start = ansi_json_get_object_field_start;
    sem->object_field_end = ansi_json_get_object_field_end;
    sem->array_element_start = ansi_json_get_array_element_start;
    sem->array_element_end = ansi_json_get_array_element_end;

    ret = pg_parse_json(lex, sem);
    if (ret == JSON_OK)
    {
        ret = state->get_json_path_result_str(*result, is_scalar, single_only, is_lax, wrap);
    }
    else
        *result = NULL;

    if (lex != NULL)
    {
        if (lex->strval != NULL)
        {
            if (lex->strval->data != NULL)
                free(lex->strval->data);
            free(lex->strval);
        }
        free(lex);
    }
    if (sem != NULL)
        NADELETEBASIC(sem, heap);
    if (state != NULL)
        NADELETEBASIC(state, heap);
    return ret;

}


static JsonReturnType
ansi_json_get_object_start(void *state, JsonTokenType toktype)
{
    JsonPathGetState  *_state = (JsonPathGetState *) state;
    int                lex_level = _state->lex->lex_level;

    _state->path_level ++;

    if (lex_level == 0 && _state->npath <= 1)
    {
        /*
         * Special case: we should match the entire object.  We only need this
         * at outermost level because at nested levels the match will have
         * been started by the outer field or array element callback.
         */
        _state->result_start = _state->lex->token_start;
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_object_end(void *state, JsonTokenType toktype)
{
    JsonPathGetState *_state = (JsonPathGetState *) state;
    int               lex_level = _state->lex->lex_level;

    _state->path_level --;

    if (lex_level == 0 && _state->npath <= 1)
    {
        /* Special case: return the entire object */
        char       *start = _state->result_start;
        int         len = _state->lex->prev_token_terminator - start;

        JsonResultElem result(_state->heap, start, len);

        _state->tresult_l.insert(result);
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_object_field_start(void *state, char *fname, bool isnull, JsonTokenType toktype)
{
    JsonPathGetState *_state = (JsonPathGetState *) state;
    bool              get_next = false;
    int               lex_level = _state->lex->lex_level;

    if (_state->path_level < _state->npath &&
            _state->path_k_ok[_state->path_level - 1] &&
            _state->paths != NULL &&
            _state->paths->getChild(_state->path_level)->isFieldKeyMatch(fname) == TRUE)
    {
        if (_state->path_level < _state->npath - 1)
        {
            /* if not at end of path just mark path ok */
            _state->path_k_ok[_state->path_level] = true;
        }
        else
        {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    if (get_next)
    {
        /* this object overrides any previous matching object */
        _state->result_start = NULL;

        if (_state->normalize_results &&
                _state->lex->token_type == JSON_TOKEN_STRING)
        {
            /* for as_text variants, tell get_scalar to set it for us */
            _state->next_scalar = true;
        }
        else
        {
            /* for non-as_text variants, just note the json starting point */
            _state->result_start = _state->lex->token_start;
        }
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_object_field_end(void *state, char *fname, bool isnull, JsonTokenType toktype)
{
    JsonPathGetState  *_state = (JsonPathGetState *) state;
    bool               get_last = false;
    int                lex_level = _state->lex->lex_level;

    /* same tests as in get_object_field_start */
    if (_state->path_level < _state->npath &&
            _state->path_k_ok[_state->path_level - 1] && 
            _state->paths != NULL &&
            _state->paths->getChild(_state->path_level)->isFieldKeyMatch(fname) == TRUE)
    {
        if (_state->path_level < _state->npath - 1)
        {
            /* done with this field so reset pathok */
            _state->path_k_ok[_state->path_level] = false;
        }
        else
        {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* for as_text scalar case, our work is already done */
    if (get_last && _state->result_start != NULL)
    {
        char          *start = _state->result_start;
        int            len = _state->lex->prev_token_terminator - start;
        bool           is_scalar = FALSE;

        is_scalar = is_json_type_scalar(toktype);

        JsonResultElem result(_state->heap, start, len, is_scalar, toktype);
        _state->tresult_l.insert(result);

        /* this should be unnecessary but let's do it for cleanliness: */
        _state->result_start = NULL;
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_array_start(void *state, JsonTokenType toktype)
{
    JsonPathGetState   *_state = (JsonPathGetState *) state;
    int                 lex_level = _state->lex->lex_level;

    _state->path_level ++;

    if (_state->path_level < _state->npath)
    {
        /* Initialize counting of elements in this array */
        _state->array_cur_index[_state->path_level] = -1;
    }

    if (lex_level == 0 && _state->npath <= 1)
    {
        /*
         * Special case: we should match the entire array.  We only need this
         * at the outermost level because at nested levels the match will
         * have been started by the outer field or array element callback.
         */
        _state->result_start = _state->lex->token_start;
    }
    else if (_state->path_level < _state->npath)
    {
        int idx_val = 0;
        int nelements;

        JsonReturnType ret = json_count_array_elements(_state->lex, nelements);
        if (JSON_OK == ret)
            _state->array_elem_count[_state->path_level] = nelements;
        else
            return ret;
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_array_end(void *state, JsonTokenType toktype)
{
    JsonPathGetState   *_state = (JsonPathGetState *) state;
    int                 lex_level = _state->lex->lex_level;

    _state->path_level --;

    if (lex_level == 0 && _state->npath <= 1)
    {
        /* Special case: return the entire array */
        char   *start = _state->result_start;
        int     len = _state->lex->prev_token_terminator - start;

        //_state->tresult = cstring_to_text_with_len(start, len);
        JsonResultElem result(_state->heap, start, len);

        _state->tresult_l.insert(result);
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_array_element_start(void *state, bool isnull, JsonTokenType toktype)
{
    JsonPathGetState   *_state = (JsonPathGetState *) state;
    bool        get_next = false;
    int         lex_level = _state->lex->lex_level;

    /* Update array element counter */
    if (_state->path_level < _state->npath)
        _state->array_cur_index[_state->path_level]++;

    if (_state->path_level < _state->npath &&
            _state->path_k_ok[_state->path_level - 1] &&
            _state->paths != NULL &&
            _state->paths->getChild(_state->path_level)->isArrayIdxMatch(_state->array_cur_index[_state->path_level]))
    {
        if (_state->path_level < _state->npath - 1)
        {
            /* if not at end of path just mark path ok */
            _state->path_k_ok[_state->path_level] = true;
        }
        else
        {
            /* end of path, so we want this value */
            get_next = true;
        }
    }

    /* same logic as for objects */
    if (get_next)
    {
        _state->result_start = NULL;

        if (_state->normalize_results &&
                _state->lex->token_type == JSON_TOKEN_STRING)
            _state->next_scalar = true;
        else
            _state->result_start = _state->lex->token_start;
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_array_element_end(void *state, bool isnull, JsonTokenType toktype)
{
    JsonPathGetState   *_state = (JsonPathGetState *) state;
    bool                get_last = false;
    int                 lex_level = _state->lex->lex_level;

    /* same tests as in get_array_element_start */
    if (_state->path_level < _state->npath &&
            _state->path_k_ok[_state->path_level - 1] &&
            _state->paths != NULL &&
            TRUE == _state->paths->getChild(_state->path_level)->isArrayIdxMatch(_state->array_cur_index[_state->path_level]))
    {
        if (_state->path_level < _state->npath - 1)
        {
            /* done with this element so reset pathok */
            _state->path_k_ok[_state->path_level] = FALSE;
        }
        else
        {
            /* end of path, so we want this value */
            get_last = true;
        }
    }

    /* same logic as for objects */
    if (get_last && _state->result_start != NULL)
    {
        char         *start = _state->result_start;
        int           len = _state->lex->prev_token_terminator - start;
        bool          is_scalar = is_json_type_scalar(toktype);

        JsonResultElem result(_state->heap, start, len, is_scalar, toktype);
        _state->tresult_l.insert(result);

        _state->result_start = NULL;
    }
    return JSON_OK;
}

static JsonReturnType
ansi_json_get_scalar(void *state, char *token, JsonTokenType tokentype)
{
    JsonPathGetState   *_state = (JsonPathGetState *) state;
    int                 lex_level = _state->lex->lex_level;

    /* Check for whole-object match */
    if (lex_level == 0 && _state->npath <= 1)
    {
        char    *start = _state->lex->input;
        int      len = _state->lex->prev_token_terminator - start;

        if (_state->normalize_results && tokentype == JSON_TOKEN_STRING)
        {
            /* we want the de-escaped string */
            _state->next_scalar = true;
        }
        else if (_state->normalize_results && tokentype == JSON_TOKEN_NULL)
        {
            JsonResultElem result(_state->heap, start, len, TRUE, tokentype);
            _state->tresult_l.insert(result);
        }
        else
        {
            /*
             * This is a bit hokey: we will suppress whitespace after the
             * scalar token, but not whitespace before it.  Probably not worth
             * doing our own space-skipping to avoid that.
             */
            JsonResultElem result(_state->heap, start, len, TRUE, tokentype);
            _state->tresult_l.insert(result);
        }
    }

    if (_state->next_scalar)
    {
        /* a de-escaped text value is wanted, so supply it */
        int len = str_len(token);

        JsonResultElem result(_state->heap, token, len, TRUE, tokentype);
        _state->tresult_l.insert(result);

        /* make sure the next call to get_scalar doesn't overwrite it */
        _state->next_scalar = false;
    }
    return JSON_OK;
}

JsonReturnType JsonPathGetState::get_json_path_result_str(char * & result, bool is_scalar = TRUE, 
                                                               bool single_only = TRUE, bool is_lax = TRUE, 
                                                               JsonWrapperBehav wrap = JSON_WITHOUT_WRAPPER)
{
    JsonReturnType ret = JSON_OK;

    int result_num = tresult_l.entries();
    int buffer_size = 0;

    if (0 == result_num && TRUE == is_lax)
    {
        return JSON_PATH_RESULT_EMPTY;//Todo: lax mode should return empty string
    }
    else if (0 == result_num && FALSE == is_lax)
    {
        return JSON_PATH_RESULT_EMPTY;
    }

    // for func json_value()
    if (TRUE == is_scalar)
    {
        if (TRUE == single_only && result_num > 1)
            return JSON_PATH_RESULT_ONLY_ONE_ELEM_ALLOW;
        else
        {
            for (int i = 0; i < result_num; i++)
            {
                if (FALSE == tresult_l[i].is_scalar())
                    return JSON_PATH_RESULT_NOT_SCALAR;
            }
        }

        NAString result_str = tresult_l[0].get_result_str();

        result = new(heap) char[result_str.length() + 1];
        str_cpy_all(result, result_str, result_str.length());
        result[result_str.length()] = '\0';

        ret = JSON_OK;
    }
    // for func json_query()/json_exists()
    else
    {
        bool do_wrap = FALSE;

        if (JSON_WITHOUT_WRAPPER == wrap)
        {
            if (result_num > 1)
                return JSON_PATH_RESULT_NEED_ARRAY_WRAPPER;
            else if (TRUE == tresult_l[0].is_scalar())
                return JSON_PATH_RESULT_NEED_ARRAY_WRAPPER;
        }
        else if (JSON_WITH_UNCONDITIONAL_WRAPPER == wrap ||
                   result_num > 1 ||
                   (JSON_WITH_CONDITIONAL_WRAPPER == wrap && FALSE == tresult_l[0].is_array()) ||
                   (JSON_WITH_CONDITIONAL2_WRAPPER == wrap && TRUE == tresult_l[0].is_scalar()))
        {
            do_wrap = TRUE;
        }

        NAString result_str(heap);
                   
        if (TRUE == do_wrap)
            result_str += "[";

        result_str += tresult_l[0].get_result_str(TRUE);
        for (int i = 1; i < result_num; i++)
        {
            result_str += ", ";
            result_str += tresult_l[i].get_result_str(TRUE);
        }

        if (TRUE == do_wrap)
            result_str += "]";

        result = new(heap) char[result_str.length() + 1];
        str_cpy_all(result, result_str, result_str.length());
        result[result_str.length()] = '\0';

        ret = JSON_OK;
    }

    return ret;
}


