
from pydantic import BaseConfig, BaseModel
from fastapi import FastAPI


from fastorm import FastORM, In, SqlFieldMeta, FieldInfo, FieldItem
from tools_for_the_tests_of_fastorm import VerboseTestCase


class SimpleTable(FastORM):
    _table_name = 'simple_table'
    _primary_keys = ['id']

    id: int
    banana: str
# end class


class FastApiTestCase(VerboseTestCase):
    def test_extending_the_table(self):
        class ClassExtendingTheTable(SimpleTable):
            extra_field: float
        # end class

        api = FastAPI()

        @api.post('/', response_model=ClassExtendingTheTable)
        def foobar():
            return ClassExtendingTheTable(
                id=12,
                banana='with chocolate',
                extra_field=0.9,
            )
        # end def

        openapi_definition = api.openapi()

        self.assertIsInstance(openapi_definition, dict)
        self.assertTrue(openapi_definition)
    # end def

    def test_containing_the_table(self):
        class ClassContainingTheTable(BaseModel):
            simple_table: SimpleTable
            extra_field: float
        # end class

        api = FastAPI()

        @api.post('/', response_model=ClassContainingTheTable)
        def foobar():
            return ClassContainingTheTable(
                simple_table=SimpleTable(
                    id=609,
                    banana='pancakes',
                ),
                extra_field=0.6,
            )
        # end def

        openapi_definition = api.openapi()

        self.assertIsInstance(openapi_definition, dict)
        self.assertTrue(openapi_definition)
    # end def
# end class
