#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <Functions/FunctionFactory.h>
#include <Poco/Logger.h>


#include "Common/Exception.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnConst.h"
#include "Columns/ColumnFunction.h"
#include "Columns/IColumn.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "DataTypes/DataTypeFunction.h"
#include "FunctionArrayMapped.h"
#include "Functions/FunctionHelpers.h"
#include "Functions/IFunction.h"
#include "Interpreters/Context_fwd.h"
#include "base/types.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int TYPE_MISMATCH;
}

class FunctionArrayFold : public IFunction, private WithContext
{
public:
    static constexpr auto name = "arrayFold";
    static FunctionPtr create(ContextPtr context_) { return std::make_shared<FunctionArrayFold>(context_); }
    explicit FunctionArrayFold(ContextPtr context_) : WithContext(context_) { }
    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    void getLambdaArgumentTypes(DataTypes & arguments) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;
};


void FunctionArrayFold::getLambdaArgumentTypes(DataTypes & arguments) const
{
    if (arguments.size() < 3)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs lambda function, at least one array argument and one accumulator argument.", getName());

    const auto lambda_arguments_size = arguments.size() - 1;
    DataTypes nested_types(lambda_arguments_size);
    /// Lambda functiong's input arguments should all be array, except last one.
    for (size_t i = 0; i < lambda_arguments_size - 1; ++i)
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument {} of function {} must be array. Found {} instead.", i + 1, getName(), arguments[i + 1]->getName());

        nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
    }
    nested_types[lambda_arguments_size - 1] = arguments[lambda_arguments_size];

    const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
    if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for this overload of {}  must be a function with {} arguments, Found {} instead.", getName(), toString(nested_types.size()), arguments[0]->getName());

    arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
}

DataTypePtr FunctionArrayFold::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    if (arguments.size() < 3)
    {
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Function {} needs at least 3 arguments; passed {}.", getName(), toString(arguments.size()));
    }

    const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());
    if (!data_type_function)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    auto const accumulator_type = arguments.back().type;
    auto const lambda_return_type = data_type_function->getReturnType();
    if (!accumulator_type->equals(*lambda_return_type))
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "Return type of lambda function must be the same as the accumulator type. "
            "Inferred type of lambda {}, inferred type of accumulator {}.", lambda_return_type->getName(), accumulator_type->getName());

    return DataTypePtr(accumulator_type);
}

ColumnPtr FunctionArrayFold::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t rows_count) const
{
    const auto & column_function_with_type_and_name = arguments[0];
    if (!column_function_with_type_and_name.column)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    const auto * column_function = typeid_cast<const ColumnFunction *>(column_function_with_type_and_name.column.get());
    if (!column_function)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

    const size_t arguments_count = arguments.size();
    assert(arguments_count > 2);
    ColumnPtr column_offset;
    ColumnPtr column_first_array;
    ColumnsWithTypeAndName arrays;
    arrays.reserve(arguments_count - 1);

    for (size_t i = 1; i < arguments_count - 1; ++i)
    {
        const auto & array_with_type_and_name = arguments[i];
        ColumnPtr column_array_ptr = array_with_type_and_name.column;
        const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        if (!column_array)
        {
            const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
            if (!column_const_array)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Expected array column, found {}", column_array_ptr->getName());
            column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
            column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
        }

        const DataTypePtr & type_array_ptr = array_with_type_and_name.type;
        const auto * type_array = checkAndGetDataType<DataTypeArray>(type_array_ptr.get());
        if (!type_array)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected array type, found {}.", type_array_ptr->getName());

        if (!column_offset)
        {
            column_offset = column_array->getOffsetsPtr();
        }
        else
        {
            /// The first condition is optimization: do not compare data if the pointers are equal.
            if (column_array->getOffsetsPtr() != column_offset
                && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*column_offset).getData())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Arrays passed to {} must have equal size.", getName());
        }
        if (i == 1)
        {
            column_first_array = column_array_ptr;
        }
        arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                  recursiveRemoveLowCardinality(type_array->getNestedType()),
                                                  array_with_type_and_name.name));
    }
    arrays.emplace_back(arguments.back());

    if (rows_count == 0)
            return arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();

    const ColumnArray::Offsets & array_offsets = checkAndGetColumn<ColumnArray>(column_first_array.get())->getOffsets();
    size_t max_array_size = array_offsets[0];
    std::vector<size_t> array_size_vec;
    array_size_vec.reserve(rows_count);
    
    std::unordered_map<size_t, ColumnsWithTypeAndName> array_size_input;
    array_size_input.reserve(max_array_size);

    for (size_t irow = 0; irow < rows_count; ++irow)
    {
        const size_t array_size = array_offsets[irow] - array_offsets[irow - 1];
        max_array_size = std::max(max_array_size, array_size);
        array_size_vec.emplace_back(array_size);
        ColumnsWithTypeAndName tmp;
        tmp.reserve(arguments_count - 1);
        array_size_input[array_size] = std::move(tmp);
    }

    for (size_t icolumn = 0; icolumn < arguments_count - 2; ++icolumn)
    {
        const auto & array_element_with_type_and_name = arrays[icolumn];
        const ColumnPtr& column_array_element_ptr = array_element_with_type_and_name.column;
        
        for (size_t isize = 1; isize <= max_array_size; ++isize)
        {
            auto column_input = column_array_element_ptr->cloneEmpty();

            for (size_t irow = 0; irow < rows_count; ++irow)
            {
                const size_t array_element_idx = array_offsets[irow - 1] + isize - 1;
                const bool insert_default = array_size_vec[irow] < isize;
                if (insert_default)
                {
                    column_input->insertDefault();
                }
                else
                {
                    // column_input->insertRangeFrom(const IColumn &src, size_t start, size_t length)
                    column_input->insertFrom((*column_array_element_ptr), array_element_idx);
                }
            }

            array_size_input[isize].emplace_back(ColumnWithTypeAndName(
                std::move(column_input), array_element_with_type_and_name.type, array_element_with_type_and_name.name));
        }
    }
    
    ColumnWithTypeAndName column_accumulator = arguments.back();
    std::vector<ColumnPtr> array_size_res;
    array_size_res.reserve(max_array_size);
    array_size_res.emplace_back(arguments.back().column);

    for (size_t array_size = 1; array_size <= max_array_size; ++array_size)
    {
        auto& columns_lambda_input = array_size_input[array_size];
        columns_lambda_input.emplace_back(column_accumulator);
        auto mutable_column_function_ptr = IColumn::mutate(column_function->getPtr());
        auto * mutable_column_function = typeid_cast<ColumnFunction *>(mutable_column_function_ptr.get());
        mutable_column_function->appendArguments(columns_lambda_input);
        auto lambda_result = mutable_column_function->reduce().column;

        if (lambda_result->lowCardinality())
            lambda_result = lambda_result->convertToFullColumnIfLowCardinality();

        column_accumulator.column = lambda_result;
        array_size_res.emplace_back(lambda_result);
    }

    MutableColumnPtr result = arguments.back().column->convertToFullColumnIfConst()->cloneEmpty();
    for (size_t i = 0; i < rows_count; ++i) {
        result->insert((*ColumnPtr(array_size_res[array_size_vec[i]]->cut(i, 1)))[0]);
    }

    return result;
}


REGISTER_FUNCTION(FunctionArrayFold)
{
    factory.registerFunction<FunctionArrayFold>(FunctionDocumentation{.description=R"(
        Function arrayFold(x1,...,xn,accum -> expression, array1,...,arrayn, init_accum) applies lambda function to a number of same sized array columns
        and collects result in accumulator. Accumulator can be either constant or column.
        )", .examples{{"sum", "SELECT arrayFold(x,acc -> acc + x, [1,2,3,4], toInt64(1));", "11"}}, .categories{"Array"}});
}

}
