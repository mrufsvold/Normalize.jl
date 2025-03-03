module ExpandedTables

using TypedTables: FlexTable

@enum ColumnStyle flat_columns nested_columns

function make_nested_table(column_set, path_graph, names...)
    @cases path_graph begin
        LeafNode(_,pool_arrays,_) => begin

        end
    end
end

end # END MODULE
