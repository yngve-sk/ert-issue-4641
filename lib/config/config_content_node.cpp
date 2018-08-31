/*
   Copyright (C) 2012  Statoil ASA, Norway.

   The file 'config_content_node.c' is part of ERT - Ensemble based Reservoir Tool.

   ERT is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   ERT is distributed in the hope that it will be useful, but WITHOUT ANY
   WARRANTY; without even the implied warranty of MERCHANTABILITY or
   FITNESS FOR A PARTICULAR PURPOSE.

   See the GNU General Public License at <http://www.gnu.org/licenses/gpl.html>
   for more details.
*/

#include <stdbool.h>
#include <string.h>

#include <ert/util/type_macros.hpp>
#include <ert/util/util.hpp>
#include <ert/util/stringlist.hpp>

#include <ert/res_util/res_env.hpp>

#include <ert/config/config_schema_item.hpp>
#include <ert/config/config_content_node.hpp>
#include <ert/config/config_path_elm.hpp>


#define CONFIG_CONTENT_NODE_ID 6752887
struct config_content_node_struct {
  UTIL_TYPE_ID_DECLARATION;
  const config_schema_item_type * schema;
  stringlist_type               * stringlist;              /* The values which have been set. */
  const config_path_elm_type    * cwd;
  stringlist_type               * string_storage;
};


static UTIL_SAFE_CAST_FUNCTION( config_content_node , CONFIG_CONTENT_NODE_ID )



config_content_node_type * config_content_node_alloc( const config_schema_item_type * schema , const config_path_elm_type * cwd) {
  config_content_node_type * node = (config_content_node_type*)util_malloc(sizeof * node );
  UTIL_TYPE_ID_INIT( node , CONFIG_CONTENT_NODE_ID );
  node->stringlist = stringlist_alloc_new();
  node->cwd = cwd;
  node->schema = schema;
  node->string_storage = NULL;
  return node;
}


void config_content_node_add_value(config_content_node_type * node , const char * value) {
  stringlist_append_copy( node->stringlist , value);
}


void config_content_node_set(config_content_node_type * node , const stringlist_type * token_list) {
  int argc = stringlist_get_size( token_list ) - 1;
  for (int iarg=0; iarg < argc; iarg++)
    config_content_node_add_value( node , stringlist_iget( token_list , iarg + 1));
}



char * config_content_node_alloc_joined_string(const config_content_node_type * node, const char * sep) {
  return stringlist_alloc_joined_string(node->stringlist , sep);
}



void config_content_node_free(config_content_node_type * node) {
  stringlist_free(node->stringlist);
  if (node->string_storage != NULL)
    stringlist_free( node->string_storage );
  free(node);
}



void config_content_node_free__(void * arg) {
  config_content_node_type * node = config_content_node_safe_cast( arg );
  config_content_node_free( node );
}

static void config_content_node_push_string( config_content_node_type * node , char * string) {
  if (node->string_storage == NULL)
    node->string_storage = stringlist_alloc_new( );

  stringlist_append_copy( node->string_storage , string );
}

const char * config_content_node_get_full_string(const config_content_node_type * node,
                                                 const char * sep) {
  char * full_string = stringlist_alloc_joined_string(node->stringlist , sep);

  config_content_node_push_string((config_content_node_type *)node, full_string );
  free(full_string);

  return stringlist_get_last(node->string_storage);
}

const char * config_content_node_iget(const config_content_node_type * node , int index) {
  return stringlist_iget( node->stringlist , index );
}


const char * config_content_node_safe_iget(const config_content_node_type * node , int index) {
  if (index >= stringlist_get_size( node->stringlist ))
    return NULL;
  else
    return stringlist_iget( node->stringlist , index );
}


config_item_types config_content_node_iget_type( const config_content_node_type * node , int index) {
  return config_schema_item_iget_type( node->schema , index );
}


time_t config_content_node_iget_as_isodate(const config_content_node_type * node , int index) {
  time_t value;
  config_schema_item_assure_type(node->schema , index , CONFIG_ISODATE);
  util_sscanf_isodate( config_content_node_iget(node , index) , &value );
  return value;
}


bool config_content_node_iget_as_bool(const config_content_node_type * node , int index) {
  bool value;
  config_schema_item_assure_type(node->schema , index , CONFIG_BOOL);
  util_sscanf_bool( config_content_node_iget(node , index) , &value );
  return value;
}


int config_content_node_iget_as_int(const config_content_node_type * node , int index) {
  int value;
  config_schema_item_assure_type(node->schema , index , CONFIG_INT);
  util_sscanf_int( config_content_node_iget(node , index) , &value );
  return value;
}



double config_content_node_iget_as_double(const config_content_node_type * node , int index) {
  double value;
  config_schema_item_assure_type(node->schema , index , CONFIG_FLOAT + CONFIG_INT);
  util_sscanf_double( config_content_node_iget(node , index) , &value );
  return value;
}




const char * config_content_node_iget_as_path(config_content_node_type * node , int index) {
  config_schema_item_assure_type(node->schema , index , CONFIG_PATH + CONFIG_EXISTING_PATH);
  {
    const char * config_value = config_content_node_iget(node , index);
    char * path_value = config_path_elm_alloc_path( node->cwd , config_value );
    config_content_node_push_string( node , path_value );

    return path_value;
  }
}


const char * config_content_node_iget_as_abspath( config_content_node_type * node , int index) {
  config_schema_item_assure_type(node->schema , index , CONFIG_PATH + CONFIG_EXISTING_PATH);
  {
    const char * config_value = config_content_node_iget(node , index);
    char * path_value = config_path_elm_alloc_abspath( node->cwd , config_value );
    config_content_node_push_string( node , path_value );

    return path_value;
  }
}


const char * config_content_node_iget_as_relpath( config_content_node_type * node , int index) {
  config_schema_item_assure_type(node->schema , index , CONFIG_PATH + CONFIG_EXISTING_PATH);
  {
    const char * config_value = config_content_node_iget(node , index);
    char * path_value = config_path_elm_alloc_relpath( node->cwd , config_value );
    config_content_node_push_string( node , path_value );

    return path_value;
  }
}

const char * config_content_node_iget_as_executable( config_content_node_type * node, int index ) {
  config_schema_item_assure_type(node->schema, index,
          CONFIG_PATH + CONFIG_EXISTING_PATH + CONFIG_EXECUTABLE );
  {
    const char * config_value = config_content_node_iget(node , index);
    char* path_value = config_path_elm_alloc_abspath( node->cwd , config_value );

    if( !strstr( config_value, UTIL_PATH_SEP_STRING )
     && !util_file_exists( path_value ) ) {
        char* tmp = res_env_alloc_PATH_executable( config_value );
        if( tmp ) {
            free( path_value );
            path_value = tmp;
        }
    }

    config_content_node_push_string( node , path_value );
    return path_value;
  }
}


const stringlist_type * config_content_node_get_stringlist( const config_content_node_type * node ) {
  return node->stringlist;
}


const char * config_content_node_get_kw( const config_content_node_type * node ) {
  return config_schema_item_get_kw( node->schema );
}




int config_content_node_get_size( const config_content_node_type * node ) {
  return stringlist_get_size( node->stringlist );
}


void config_content_node_assert_key_value( const config_content_node_type * node ) {
  int argc_min , argc_max;
  config_schema_item_get_argc( node->schema , &argc_min , &argc_max);

  if (!((argc_min == 1) && (argc_min == 1)))
    util_abort("%s: item:%s before calling config_get_value() functions *without* index you must set argc_min == argc_max = 1 \n",__func__ , config_schema_item_get_kw( node->schema ));
}


const config_path_elm_type * config_content_node_get_path_elm( const config_content_node_type * node ) {
  return node->cwd;
}

/**
   The node should contain elements of the type:

      KEY1:VALUE1    KEY2:Value2   XX Key3:Val3  Ignored

   Which will be inserted in the opt_hash dictionary as : {"KEY1" :
   "VALUE1" , ... } Elements which do not conform to this syntax are
   ignored.
*/


void config_content_node_init_opt_hash( const config_content_node_type * node , hash_type * opt_hash , int elm_offset) {
  int i;
  for (i = elm_offset; i < config_content_node_get_size( node ); i++)
    hash_add_option( opt_hash , config_content_node_iget( node , i ));
}


void config_content_node_fprintf( const config_content_node_type * node , FILE * stream ) {
  fprintf(stream , "%s: {" , config_schema_item_get_kw( node->schema ));
  stringlist_fprintf( node->stringlist , ", " , stream );
  fprintf(stream , "}\n");
}
