
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	iter = input;
	this->condition = condition;
	attrValue = malloc(PAGE_SIZE);

	attrs.clear();
	input->getAttributes(attrs);
}


RC Filter::getNextTuple(void* data){
	RC rc;
	while((rc = iter->getNextTuple(data)) == SUCCESS){
		//if no condition is specified, return any tuple found
		if(condition.op == NO_OP) break;

		//otherwise retrieve the condition attribute value and verify if condition is met
		if(getValue(condition.lhsAttr, attrs, data, attrValue))
			continue;
		if(checkCondition(condition.rhsValue.type, attrValue, condition.op, condition.rhsValue.data))
			break;
	}
	return rc;
}

void Filter::getAttributes(vector<Attribute> &attrs) const{
	attrs = this->attrs;
}

bool Filter::checkCondition(AttrType type, void* left, CompOp op, void* right) {            
    switch (type) {

        case TypeVarChar: {
            int32_t valueSize;
    		memcpy(&valueSize, left, VARCHAR_LENGTH_SIZE);
    		char leftStr[valueSize + 1];
    		leftStr[valueSize] = '\0';
    		memcpy(leftStr, (char*) left + VARCHAR_LENGTH_SIZE, valueSize);
            return checkCondition(leftStr, op, right);
        }

        case TypeInt: return checkCondition(*(int*)left, op, right);
        case TypeReal: return checkCondition(*(float*)left, op, right);
        default: return false;
    }            
};


//---------------Helper functions for Filter::checkCondition()-----------------//

bool Filter::checkCondition(int recordInt, CompOp compOp, const void *value)
{
    int32_t intValue;
    memcpy (&intValue, value, INT_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordInt == intValue;
        case LT_OP: return recordInt < intValue;
        case GT_OP: return recordInt > intValue;
        case LE_OP: return recordInt <= intValue;
        case GE_OP: return recordInt >= intValue;
        case NE_OP: return recordInt != intValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkCondition(float recordReal, CompOp compOp, const void *value)
{
    float realValue;
    memcpy (&realValue, value, REAL_SIZE);

    switch (compOp)
    {
        case EQ_OP: return recordReal == realValue;
        case LT_OP: return recordReal < realValue;
        case GT_OP: return recordReal > realValue;
        case LE_OP: return recordReal <= realValue;
        case GE_OP: return recordReal >= realValue;
        case NE_OP: return recordReal != realValue;
        case NO_OP: return true;
        // Should never happen
        default: return false;
    }
}

bool Filter::checkCondition(char *recordString, CompOp compOp, const void *value)
{
    if (compOp == NO_OP)
        return true;

    int32_t valueSize;
    memcpy(&valueSize, value, VARCHAR_LENGTH_SIZE);
    char valueStr[valueSize + 1];
    valueStr[valueSize] = '\0';
    memcpy(valueStr, (char*) value + VARCHAR_LENGTH_SIZE, valueSize);

    int cmp = strcmp(recordString, valueStr);
    switch (compOp)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        // Should never happen
        default: return false;
    }
}

//-----------------------------------------------------------------------------------//
