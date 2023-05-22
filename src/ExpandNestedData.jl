module ExpandNestedData
using PooledArrays
using StructTypes

export expand
export ColumnDefinition
export nested_columns, flat_columns

# Link a list of keys into an underscore separted column name
join_names(names, joiner="_") = names .|> string |> (s -> join(s, joiner)) |> Symbol

include("ExpandTypes.jl")
include("ExpandedTable.jl")
include("Processing.jl")

end
