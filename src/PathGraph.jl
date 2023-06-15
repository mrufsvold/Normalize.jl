module PathGraph
using SumTypes
using ..ColumnSetManagers: ColumnSetManager, NameID, get_id
using ..NestedIterators: NestedIterator
import ..get_name
export Node, SimpleNode, ValueNode, PathNode, get_name, get_children, get_all_value_nodes

@sum_type Node :hidden begin
    Path(::NameID, ::Vector{Node})
    Value(::NameID, ::NameID, ::NameID, ::Bool, ::Ref{NestedIterator{<:Any, <:Any}})
    Simple(::NameID)
end

PathNode(csm::ColumnSetManager, name, children::Vector{Node}) = PathNode(get_id(csm, name), children)
PathNode(name::NameID, children::Vector{Node}) = Node'.Path(name, children)

function ValueNode(csm::ColumnSetManager, name, final_name, field_path, pool_arrays::Bool, default::NestedIterator)
    ValueNode(get_id(csm, name),  get_id(csm, final_name),  get_id(csm, field_path), pool_arrays, default)
end
ValueNode(name::NameID, final_name::NameID, field_path::NameID, pool_arrays::Bool, default::NestedIterator) = Node'.Value(name, final_name, field_path, pool_arrays, Ref{NestedIterator{<:Any, <:Any}}(default))

SimpleNode(csm::ColumnSetManager, name) = SimpleNode(get_id(csm, name))
SimpleNode(name::NameID) = Node'.Simple(name)

function get_name(node::Node)
    return @cases node begin 
        Path(n,_) => n
        Value(n,_,_,_,_) => n
        Simple(n) => n
    end
end
function get_children(node::Node)
    return @cases node begin 
        Path(_,c) => c
        [Value,Simple] => throw(ErrorException("Value and Simple nodes do not have children"))
    end
end
function get_final_name(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a final_name"))
        Value(_,n,_,_,_) => n
    end
end
function get_field_path(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a field_path"))
        Value(_,_,p,_,_) => p
    end
end
function get_pool_arrays(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a pool_arrays"))
        Value(_,_,_,p,_) => p
    end
end
function get_default(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a default"))
        Value(_,_,_,_,d) => p
    end
end


"""Given a certain level index, return the rest of the path down to the value"""
function path_to_value(c::Node, current_index)
    fp = get_field_path(c)
    return fp[current_index:end]
end

function get_all_value_nodes(node::Node)
    value_node_channel = Channel{Node}() do ch
        get_all_value_nodes(node, ch)
    end
    return collect(value_node_channel)
end
function get_all_value_nodes(node::Node, ch)
    @cases node begin
        Path => get_all_value_nodes.(get_children(node), Ref(ch))
        Value => put!(ch, node)
        Simple => throw(ErrorException("Cannot retrieve value nodes from a simple node"))
    end
    return nothing
end


function make_path_nodes!(column_defs, level = 1)
    unique_names = get_unique_current_names(column_defs, level)
    nodes = Vector{Node}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_defs = filter(p -> current_path_name(p, level) == unique_name, column_defs)
        are_value_nodes = [!has_more_keys(def, level) for def in matching_defs]
        
        all_value_nodes = all(are_value_nodes)
        mix_of_node_types = !all_value_nodes && any(are_value_nodes)

        if all_value_nodes
            # If we got to a value node, there should only be one.
            def = first(matching_defs)
            nodes[i] = wrap(ValueNode(
                unique_name, get_field_path(def), get_pool_arrays(def), NestedIterator(get_default_value(def));
                col_name = get_column_name(def)))
            continue
        end

        with_children = !mix_of_node_types ? 
            matching_defs :
            [def for (is_value, def) in zip(are_value_nodes, matching_defs) if !is_value]
        children_column_defs = make_column_def_child_copies(with_children, unique_name, level)

        child_nodes = make_path_nodes!(children_column_defs, level+1)
        if mix_of_node_types
            without_child_idx = findfirst(identity, are_value_nodes)
            without_child = matching_defs[without_child_idx]
            value_column_node = ValueNode(
                unnamed(), 
                (get_field_path(without_child)..., unnamed()), 
                get_pool_arrays(without_child),
                NestedIterator(get_default_value(without_child));
                col_name=get_column_name(without_child))
            push!(child_nodes, wrap(value_column_node))
        end

        nodes[i] = wrap(PathNode(unique_name, child_nodes))
    end
    return nodes
end 


"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(column_defs) = wrap(PathNode(:TOP_LEVEL, make_path_nodes!(column_defs)))
make_path_graph(::Nothing; _...) = wrap(SimpleNode(nothing))

end