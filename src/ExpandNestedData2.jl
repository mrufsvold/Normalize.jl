module ExpandNestedData2
using Accessors: @set
using ScopedValues: ScopedValue, with
using AutoHashEquals: @auto_hash_equals
using SumTypes: @sum_type, @cases
using StructTypes: StructTypes
using PooledArrays: PooledArray
using TypedTables: FlexTable


NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}

@enum ColumnStyle flat_columns nested_columns
@enum PoolArrayOptions NEVER ALWAYS AUTO

const DEFAULT_MISSING = ScopedValue(missing)

include("PathGraph2.jl")

if false
    IterCapture = nothing
    T = nothing
    IC = nothing
    Seed = nothing
    Repeat = nothing
    Cycle = nothing
    Concat = nothing
end


###### Name Path Types
@auto_hash_equals cache = true struct NamePart
    name
end
NamePart(;name=nothing) = NamePart(name)

@auto_hash_equals struct NamePath
    parts::Vector{NamePart}
end
NamePath() = NamePath(NamePart[])

function append(np::NamePath, name)
    new_parts = copy(np.parts)
    push!(new_parts, NamePart(name))
    return NamePath(new_parts)
end
function Base.string(np::NamePath)
    return join((np.parts[i].name for i in 1:np.len), ".")
end
function Base.getindex(np::NamePath, i::Int)
    return np.parts[i].name
end
function Base.length(np::NamePath)
    return length(np.parts)
end
function Base.lastindex(np::NamePath)
    return length(np.parts)
end
function Base.firstindex(np::NamePath)
    return 1
end


"""
    get_unique_current_names(name_paths, level)
Get all unique names for the given depth level for a list of `NamePath`s
"""
get_unique_current_names(name_paths, level) = unique((current_path_name(name_path, level) for name_path in name_paths))

current_path_name(name_path::NamePath, level) = name_path.parts[level]

###### Iter Instruction Types
@sum_type IterCapture{T} <: AbstractVector{T} :hidden begin
    Seed{T}(data::T)
    Repeat{T}(len::Int, child::IterCapture{T}, n::Int)
    Cycle{T}(len::Int, child::IterCapture{T})
    Concat(len::Int, children_n::Int, children::Vector{Pair{Int,IterCapture}})
end
function get_children(ic::IterCapture)
    @cases ic begin
        Concat(_, _, children) => map(last, children)
        [Repeat, Cycle](_, child,_) => (child,)
        Seed => nothing
    end
end

Base.eltype(::IterCapture{T}) where T = T

function Base.length(ic::IterCapture{T}) where T
    @cases ic begin
        Seed => 1
        [Repeat, Cycle, Concat](len, _...) => len
    end
end

function get_all_seeds(ic::IterCapture, up_to::Int=64)
    seeds = @cases ic begin
        Seed(data) => Set((data,))
        [Repeat, Cycle,](_, child) => get_all_seeds(child)
        Concat(_, _, children) => union((get_all_seeds(last(child)) for child in children)...)
    end

    return if isnothing(seeds)
        nothing
    elseif up_to < length(seeds)
        nothing
    else
        seeds
    end
end



Base.size(ic::IterCapture) = (length(ic),)
seed(data::T) where T = IterCapture'.Seed{T}(data)
repeat(ic::IterCapture{T}, n::Int) where T= IterCapture'.Repeat{T}(n*length(ic), ic, n)
cycle(ic::IterCapture{T}, n::Int) where T = IterCapture'.Cycle{T}(n*length(ic), ic)
function concat(ics)
    T = Union{eltype.(ics)...}
    n = length(ics)
    final_indices = accumulate(+, length.(ics))
    children = Pair{Int,IterCapture}[
        i => ic
        for (i, ic) in Iterators.zip(final_indices, ics)
    ]
    len = last(final_indices)
    res::IterCapture{T} = IterCapture'.Concat(len, n, children)
    return res
end
concat(ics::IterCapture...) = concat(ics)
function unconcat(i::Int, children::Vector{Pair{Int,IterCapture}}, ::Type{E}) where E
    child_index = searchsortedfirst(children, (i,); by=first)
    prev_last_index = child_index == 1 ? 0 : first(children[child_index-1])
    child = last(children[child_index])
    child[i-prev_last_index]
end
function Base.getindex(current_ic::IterCapture{T}, i::Int) where T
    length(current_ic) < i && throw(BoundsError(current_ic, i))
    return @cases current_ic begin
        Seed(data) => data::T
        Repeat(len, ic, n) => ic[ceil(Int64, i/n)]
        Cycle(len, ic) => ic[mod((i-1), length(ic)) + 1]
        Concat(_, n, children) => unconcat(i, children, T)
    end
end

###### Column Types
struct Column
    name::NamePath
    data::IterCapture
end
Base.length(c::Column) = length(c.data)
Base.eltype(c::Column) = eltype(c.data)
function Base.collect(c::Column; pool_arrays)
    if pool_arrays == AUTO
        pool_up_to = length(c) ÷ 5
        seeds = get_all_seeds(c.data, pool_up_to)
        if !isnothing(seeds)
            return PooledArray(c.data)
        end
    elseif pool_arrays == ALWAYS
        return PooledArray(c.data)
    end
    return collect(c.data)
end
function get_name_path(c::Column)
    return c.name
end

function expand(data;
        pool_arrays=false,
        lazy_columns=false,
        name_join_pattern="_",
        column_style=:flat
    )
    col_set = _expand(data, NamePath())

    if column_style == :flat
        return FlexTable(;
            (
                join_name_path(c.name, name_join_pattern) => lazy_columns ? c.data : collect(c; pool_arrays=pool_arrays)
                for c in col_set
            )...
        )
    end

    name_paths = get_name_path.(col_set)
    path_graph = make_path_graph(name_paths)
    make_nested_table(col_set, path_graph)
end

function join_name_path(np::NamePath, join_pattern)
    parts = Iterators.map(p -> string(p.name), np.parts)
    joined = join(parts, join_pattern)
    return Symbol(joined)
end

function _expand(@nospecialize(data), name_path)
    T = typeof(data)
    StructT = typeof(StructTypes.StructType(T))
    if StructT <: StructTypes.DictType
        return _expand_dict(data, name_path)
    elseif StructT <: StructTypes.DataType
        return _expand_data_type(data, name_path)
    elseif StructT <: StructTypes.ArrayType
        return _expand_array(data, name_path)
    else
        return _expand_leaf(data, name_path)
    end
end

function _expand_dict(@nospecialize(data), name_path::NamePath)
    return _expand_name_value_container(data, keys(data), getindex, name_path)
end

function _expand_data_type(@nospecialize(data), name_path::NamePath)
    return _expand_name_value_container(data, propertynames(data), getproperty, name_path)
end

function _expand_name_value_container(@nospecialize(data), @nospecialize(names), getter, name_path::NamePath)
    if length(names) == 0
        return Column[]
    end

    list_of_column_sets = Vector{Column}[
        _expand(getter(data, name), append(name_path, name))
        for name in names
    ]
    return merge_columns!(list_of_column_sets)
end

function merge_columns!(list_of_column_sets)
    column_set = pop!(list_of_column_sets)
    multiplier = length(column_set[1])
    while length(list_of_column_sets) > 0
        new_column_set = pop!(list_of_column_sets)
        if length(new_column_set) == 0
            continue
        end
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeated_column_set = map(c -> cycle_column(c, multiplier), new_column_set)
        multiplier = length(repeated_column_set[1])
        append!(column_set, repeated_column_set)
    end
    return map(c -> cycle_column(c, multiplier ÷ length(c)), column_set)
end

function cycle_column(column, n)
    return Column(
        column.name,
        cycle(column.data, n)
        )
end

function Base.vcat(columns::Column...)
    allequal(c.name for c in columns) || throw(ArgumentError("columns must have the same name"))
    return Column(
        columns[1].name,
        concat((c.data for c in columns)...)
        )
end

function _expand_array(@nospecialize(data), name_path)
    if isempty(data)
        return Column[Column(name_path, seed(DEFAULT_MISSING[]))]
    end
    expanded = map(_expand, data, Iterators.repeated(name_path))
    no_empties = filter(!isempty, expanded)
    all_names = Set(Iterators.flatmap(c -> (x.name for x in c), no_empties))
    return Column[stack_columns(no_empties, name) for name in all_names]
end

function stack_columns(column_sets, name)
    data = concat(map(c -> get_column(c, name).data, column_sets))
    Column(name, data)
end



function get_column(column_set, name_path)
    len = length(column_set[1])
    i = findfirst(c -> c.name == name_path, column_set)
    if isnothing(i)
        x = DEFAULT_MISSING[]
        return cycle_column(Column(name_path, seed(x)), len)
    end
    return column_set[i]
end

function _expand_leaf(@nospecialize(data), name_path::NamePath)
    return Column[Column(name_path, seed(data))]
end


function make_nested_table(column_set, path_graph::PathNode, name_path::NamePath=NamePath())
    return @cases path_graph begin
        [TopLevelNode, BranchNode] => table_from_children(column_set, path_graph, name_path)
        LeafNode(name, _, pool_arrays, _) => collect(
            get_column(column_set, name_path); pool_arrays=pool_arrays)
    end
end

function table_from_children(column_set, path_graph, name_path)
    children = get_children(path_graph)
    return FlexTable(;
        (
            Symbol(string(get_name(child))) =>make_nested_table(
                column_set, child, append(name_path, get_name(child))
            )
            for child in children
        )...
    )
end

end # END MODULE HERE #
