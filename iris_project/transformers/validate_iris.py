
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def validate(df, *args, **kwargs):
    print("Missing values:", df.isnull().sum().sum())
    print("Number of classes:", df['target'].nunique())
    return df


@test
def test_output(output, *args):
    assert output.isnull().sum().sum() == 0, "Dataset contains missing values"
    assert output['target'].nunique() == 3, "Dataset must contain exactly 3 classes"
