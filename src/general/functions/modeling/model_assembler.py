"""Model assembler"""


def assembler(data, transformer):
    model = transformer.fit(data)
    model.transform(data)
    return transformer.fit(data)
    transformer.transform(data)
