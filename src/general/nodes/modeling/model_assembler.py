"""Model assembler pre-function"""

from general.functions.modeling import model_assembler
from utilities.objects import initializes_objects


@initializes_objects
def assembler(*args, **kwargs):
    return model_assembler.assembler(*args, **kwargs)
