import six
import logging

import tensorflow as tf
from tensorflow.keras.regularizers import l2

from src.pbs import pipeline_pb2
from src.utils.load_class import get_register_class_meta
from src.deeplearning.layers.input_layer import InputLayer


logger = tf.get_logger()
logger = logger.setLevel(logging.WARN)

_EASY_REC_MODEL_CLASS_MAP = {}
_meta_type = get_register_class_meta(
    _EASY_REC_MODEL_CLASS_MAP, have_abstract_class=True)


class Model(six.with_metaclass(_meta_type, object)):

    def __init__(self, model_config, feature_configs, features, label=None, is_training=False):
        self._base_model_config = model_config
        self._model_config = model_config
        self._feature_dict = features
        self._is_training = is_training

        self._emb_reg = l2(self.embedding_regularization)
        self._l2_reg = l2(self.l2_regularization)
        # only used by model with wide feature groups, e.g. WideAndDeep
        self._wide_output_dim = -1

        self._feature_configs = feature_configs
        self.build_input_layer(model_config, feature_configs)


    @property
    def embedding_regularization(self):
        return self._base_model_config.embedding_regularization

    @property
    def l2_regularization(self):
        model_config = getattr(self._base_model_config,
                               self._base_model_config.WhichOneof('model'))
        l2_regularization = 0.0
        if hasattr(model_config, 'l2_regularization'):
            l2_regularization = model_config.l2_regularization
        return l2_regularization


    def build_input_layer(self, model_config, feature_config):
        self._input_layer = InputLayer(
            feature_config,
            model_config.feature_groups,
            wide_output_dim=self._wide_output_dim,
            use_embedding_variable=model_config.use_embedding_variable,
            embedding_regularizer=self._emb_reg,
            kernel_regularizer=self._l2_reg,
            variational_dropout_config=model_config.variational_dropout
            if model_config.HasField('variational_dropout') else None,
            is_training=self._is_training)
