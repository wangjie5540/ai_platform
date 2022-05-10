# coding: utf-8
from kfp.dsl import ContainerOp
from . import uplift_model


class UpliftOp(ContainerOp):
    def __init__(self, input_file):
        super(UpliftOp, self).__init__(
            name='uplift op',
            image='digit-force-docker.pkg.coding.net/marketing_algorithm/hello-world/my_test_component:latest',
            command=[],
            arguments=[
                '--input_file',
                input_file,
            ],
            file_outputs={
                'model': uplift_model.output_file
            },
        )
