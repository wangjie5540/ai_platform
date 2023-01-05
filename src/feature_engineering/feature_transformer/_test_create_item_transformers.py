import transformer

transformer_rules = [
    {
        "type": "string_indexer",
        "input_col": "item_id",
        "output_col": "item_id_index",
    },
    {
        "type": "string_indexer",
        "input_col": "fund_type",
        "output_col": "fund_type_index",
    },
    {
        "type": "string_indexer",
        "input_col": "management",
        "output_col": "management_index",
    },
    {
        'type': 'string_indexer',
        'input_col': 'custodian',
        'output_col': 'custodian_index',
    },
    {
        'type': 'string_indexer',
        'input_col': 'invest_type',
        'output_col': 'invest_type_index',
    },
    {
        'type': 'standard_scaler',
        'input_col': 'i_buy_counts_30d',
        'output_col': 'i_buy_counts_30d_scaled',
    }
]

transformer.create(table_name="algorithm.tmp_raw_item_feature_table_name", transform_rules=transformer_rules,
                   name='item_feature_transformers')
