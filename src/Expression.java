import storageManager.FieldType;
import storageManager.Tuple;


public class Expression {
    class Temp {
        String type;
        String tempString;
        int tempInteger;
        public boolean equals(Temp t2) {
            if (!this.type.equalsIgnoreCase(t2.type)) return false;
            if(this.type.equals("INT")) {
                return this.tempInteger == t2.tempInteger;
            } else {
                return this.tempString.equals(t2.tempString);
            }
        }
    }

    Statement exp_statement;
    public Expression(Statement exp_statement) {
        this.exp_statement = exp_statement;
    }

    public boolean evaluateBoolean(Tuple tuple) {

        switch (exp_statement.getContent()) {
            case "EXPRESSION":
            {
                
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateBoolean(tuple);
                
            }
            case "AND": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateBoolean(tuple)
                        &&new Expression(exp_statement.getSubstatement().get(1)).evaluateBoolean(tuple);
            }
            case "OR": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateBoolean(tuple)
                        ||new Expression(exp_statement.getSubstatement().get(1)).evaluateBoolean(tuple);
            }
            case "=": {
                Expression left = new Expression(exp_statement.getSubstatement().get(0));
                Expression right = new Expression(exp_statement.getSubstatement().get(1));
                return left.evaluateUnknown(tuple).equals(right.evaluateUnknown(tuple));
            }
            case ">": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        >new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "<": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        <new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "NOT": {
                return !new Expression(exp_statement.getSubstatement().get(0)).evaluateBoolean(tuple);
            }
            default: try {
                throw new Exception("Unknown Operator");
            }catch (Exception err) {
                err.printStackTrace();
            }
        }
        return false;
    }

    public int evaluateInt(Tuple tuple) {
        switch (exp_statement.getContent()) {
            case "+": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        + new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "-": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        - new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "*": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        * new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "/": {
                return new Expression(exp_statement.getSubstatement().get(0)).evaluateInt(tuple)
                        / new Expression(exp_statement.getSubstatement().get(1)).evaluateInt(tuple);
            }
            case "FieldName": {
                StringBuilder fieldName = new StringBuilder();
                for (Statement name: exp_statement.getSubstatement()) {
                    fieldName.append(name.getContent()+".");
                }
                fieldName.deleteCharAt(fieldName.length()-1);
                String name = fieldName.toString();
                return tuple.getField(name).integer;
            }
            case "INT": {
                return Integer.parseInt(exp_statement.getSubstatement().get(0).getContent());
            }
        }

        return 0;
    }

    public Temp evaluateUnknown(Tuple tuple) {
        Temp temp = new Temp();
        if (exp_statement.getContent().equalsIgnoreCase("STRING")) {
            temp.type = "STRING";
            temp.tempString = exp_statement.getSubstatement().get(0).getContent();
        } else if (exp_statement.getContent().equalsIgnoreCase("INT")) {
            temp.type = "INT";
            temp.tempInteger = Integer.parseInt(exp_statement.getSubstatement().get(0).getContent());
        } else if (exp_statement.getContent().equalsIgnoreCase("FieldName")) {
            StringBuilder fieldName = new StringBuilder();
            for (Statement name: exp_statement.getSubstatement()) {
                fieldName.append(name.getContent()+".");
            }
            fieldName.deleteCharAt(fieldName.length()-1);
            String name = fieldName.toString();
            FieldType type = tuple.getSchema().getFieldType(name);
            if (type == FieldType.INT) {
                temp.type = "INT";
                temp.tempInteger = tuple.getField(name).integer;
            } else {
                temp.type = "STRING";
                temp.tempString = tuple.getField(name).str;
            }
        } else {
            temp.type = "INT";
            temp.tempInteger = evaluateInt(tuple);
        }
        return temp;
    }

}
