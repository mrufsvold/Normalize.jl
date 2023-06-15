is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer
is_container(t) = typeof(StructTypes.StructType(t)) <: Container
is_value_type(t::Type) = !is_container(t) && isconcretetype(t)

"""Check if the eltype of a T are all value types (i.e. not containers)"""
all_eltypes_are_values(::Type{T}) where T = all_is_value_type(eltype(T))
function all_is_value_type(::Type{T}) where T
    if T isa Union
        return all(is_value_type.(Base.uniontypes(T)))
    end
    return is_value_type(T)
end


"""Get the keys/names of any NameValueContainer"""
@generated function get_names(x::T) where T
    struct_t = StructTypes.StructType(T)
    if struct_t isa StructTypes.DataType
        return :((n for n in fieldnames(T)))
    elseif struct_t isa StructTypes.DictType
        return :(keys(x))
    end
    return :(TypeError(:get_names, "Expected a dict or struct", NameValueContainer, T))
end

"""Get the value for a key of any NameValueContainer. If it does not have the key, return default"""
@generated function get_value(x::T, name, default) where T
    struct_t = StructTypes.StructType(T)
    if struct_t isa StructTypes.DataType
        return :(hasproperty(x, name) ? getproperty(x, name) : default)
    elseif struct_t isa StructTypes.DictType
        return :(get(x, name, default))
    end
    return :(TypeError(:get_names, "Expected a dict or struct", NameValueContainer, T))
end

"""Link a list of keys into an underscore separted column name"""
join_names(names, joiner="_") = names .|> string |> (s -> join(s, joiner)) |> Symbol


# Doing some type piracy to patch a bug in DataStructures.jl
function Base.setindex!(h::OrderedRobinDict{K, V}, v0, key0) where {K,V}
    key = convert(K, key0)
    v = convert(V, v0)
    index = get(h.dict, key, -2)

    if index < 0
        DataStructures._setindex!(h, v0, key0)
    else
        @assert haskey(h, key0)
        @inbounds orig_v = h.vals[index]
        !isequal(orig_v, v0) && (@inbounds h.vals[index] = v0)
    end

    DataStructures.check_for_rehash(h) && DataStructures.rehash!(h)

    return h
end