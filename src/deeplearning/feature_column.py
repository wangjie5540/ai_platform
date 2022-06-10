from collections import namedtuple


SparseFeat = namedtuple('SparseFeat',
                        ['name', 'vocab_size', 'hash_size', 'share_emb', 'emb_dim', 'dtype'])
VarLenFeat = namedtuple('VarLenFeat',
                        ['name', 'vocab_size', 'hash_size', 'share_emb', 'seq_multi_sep', 'weight_name', 'emb_dim',
                         'max_len', 'combiner', 'dtype', 'sub_dtype'])
DenseFeat = namedtuple('DenseFeat',
                       ['name', 'dim', 'share_emb', 'dtype'])
BucketFeat = namedtuple('BucketFeat',
                        ['name', 'boundaries', 'share_emb', 'emb_dim', 'dtype'])
