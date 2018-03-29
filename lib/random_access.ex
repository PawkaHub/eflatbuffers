defmodule Eflatbuffers.RandomAccess do
  alias Eflatbuffers.Utils

  require Logger

  # XXX: IMPORTANT ###
  # XXX: This is a rough work-around but it'll let us get union payload functionality working as expected for now. It's important that we come back later and clean this code up so that it function more cleanly with the rest of the codebase.
  def get_union(data, [path_key | path_keys], union_name, {tables, %{root_type: root_type}} = schema) when is_atom(path_key) do

    #Logger.debug "path_key #{path_key}"
    #Logger.debug "root_type #{inspect root_type}"

    # Get the root table
    {:table, table_options} = Map.get(tables, root_type)
    #Logger.debug "table_options #{inspect table_options}"

    # Get the indexes for the path at the root table
    {index, path_type} = Map.get(table_options.indices, path_key)
    #Logger.debug "path_type #{inspect path_type} #{index}"

    # Find the union values
    union_result = case path_type do
      {:union, %{name: union_path_name}} ->
        #Logger.debug "union_path_name #{inspect union_path_name}"

        # Get the union definition
        {:union, union_definition} = Map.get(tables, union_path_name)
        #Logger.debug "union_definition #{inspect union_definition}"

        # Get the index of the destination union
        union_type_index = Map.get(union_definition.members, union_name)
        #Logger.debug "union_type_index #{inspect union_type_index}"

        # We add one as the union data is always directly after the indice (we pass in zero here as tat's the root buffer type that we want to be working with)
        union_data_pointer = data_pointer(index + 1, 0, data)
        #Logger.debug "union_data_pointer #{inspect union_data_pointer}"

        # Now that we've got the union type index just do a read directly as we have all the values we need to get the data we want
        Eflatbuffers.Reader.read({:table, %{name: union_name}}, union_data_pointer, data, schema)
      _ ->
        # XXX: Throw an error for now (we'll make this better later)
        raise "Something went wrong while trying to find a union in the get_union function in the RandomAccess module"
    end

    union_result
  end
  ### END IMPORTANT ###

  def get([], root_table, 0, data, schema) do
    #Logger.debug "empty array get called"
    Eflatbuffers.Reader.read(root_table, 0, data, schema)
  end

  def get([key | keys], {:table, %{ name: table_name }}, table_pointer_pointer, data, {tables, _} = schema) when is_atom(key) do
    {:table, table_options} = Map.get(tables, table_name)
    {index, type} = Map.get(table_options.indices, key)
    #Logger.debug "type #{inspect type}"

    {type_concrete, index_concrete} =
    case type do
      {:union, %{name: union_name}} ->
        #Logger.debug "Sigh #{union_name}"
        # we are getting the field type from the field
        # and the data is actually in the next field
        # since the schema does not contain the *_type field
        #Logger.debug "index #{index}"
        #Logger.debug "table_pointer_pointer #{table_pointer_pointer}"
        type_pointer = data_pointer(index, table_pointer_pointer, data)
        #Logger.debug "type_pointer #{inspect type_pointer}"
        #Logger.debug "tables #{inspect tables}"
        {:union, union_definition} = Map.get(tables, union_name)
        #Logger.debug "union_definition #{inspect union_definition.members}"

        # Make it whatever table index we want
        union_type_index = Eflatbuffers.Reader.read({:byte, %{ default: 0 }}, type_pointer, data, schema)

        # Logger.debug "union_type_index #{union_type_index}"
        union_type = Map.get(union_definition.members, union_type_index)
        # union_type = Map.get(union_definition.members, union_type_index)
        type = {:table, %{ name: union_type }}
        #Logger.debug "type #{inspect type}"
        {type, index + 1}
      _ ->
        {type, index}
    end

    #Logger.debug "starting data_pointer #{inspect type_concrete} #{index_concrete} #{table_pointer_pointer}"
    case data_pointer(index_concrete, table_pointer_pointer, data) do
      false ->
        # we encountered a null pointer, we return nil
        # whether we reached the end of the path or not
        #Logger.debug "Nil?"
        nil
      data_pointer ->
        case keys do
          [] ->
            # this is the terminus where we switch to eager reading
            #Logger.debug "Start reading #{inspect type_concrete} #{data_pointer}"
            read_result = Eflatbuffers.Reader.read(type_concrete, data_pointer, data, schema)
            #Logger.debug "read_result #{inspect read_result}"
            read_result
          _ ->
            # there are keys left, we recurse
            get(keys, type_concrete, data_pointer, data, schema)
        end
    end
  end

  def get([index | keys], {:vector, %{ type: type }}, vector_pointer, data, schema) when is_integer(index) do
    << _ :: binary-size(vector_pointer), vector_offset :: unsigned-little-size(32), _ :: binary >> = data
    vector_length_pointer = vector_pointer + vector_offset
    << _ :: binary-size(vector_length_pointer), vector_length :: unsigned-little-size(32), _ :: binary >> = data
    element_offset =
    case Utils.scalar?(type) do
      true ->
         Utils.scalar_size(Utils.extract_scalar_type(type, schema))
      false ->
        4
    end

    data_offset =  vector_length_pointer + 4 + index * element_offset
    case vector_length < index + 1 do
      true ->
        throw(:index_out_of_range)
      false ->
        case keys do
          [] ->
            Eflatbuffers.Reader.read(type, data_offset, data, schema)
          _ ->
            get(keys, type, data_offset, data, schema)
        end
    end
  end

  def data_pointer(index, table_pointer_pointer, data) do
    << _ :: binary-size(table_pointer_pointer), table_offset :: little-size(32), _ :: binary >> = data
    table_pointer     = table_pointer_pointer + table_offset
    << _ :: binary-size(table_pointer), vtable_offset :: little-signed-size(32), _ :: binary >> = data
    vtable_pointer = table_pointer - vtable_offset + 4 + index * 2
    << _ :: binary-size(vtable_pointer),  data_offset :: little-size(16), _ :: binary >> = data
    case data_offset do
      0 -> false
      _ -> table_pointer + data_offset
    end
  end

  def index_and_type(fields, key) do
    {{^key, type}, index} = Enum.find(Enum.with_index(fields), fn({{name, _}, _}) -> name == key end)
    {index, type}
  end

end
