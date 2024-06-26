import java.util.ArrayList;

public class Utils_1NN {

	private static final String[] attributes = new String[] {
			"id","age","sex","cp","trestbps","chol","fbs","restecg",
			"thalach","exang","oldpeak","slope","ca","thal","diagnosis","dataset"
	};

	public static String getAttribute(String[] row, String attribute) {
		for (int i = 0; i < attributes.length; i++) {
			if (attributes[i].equals(attribute)) {
				return row[i];
			}
		}
		return null;
	}

	public static ArrayList<String> getPredictors() {
		ArrayList<String> predictors = new ArrayList<String>();
		for (int i = 0; i < attributes.length; i++) {
			if (!attributes[i].equals("id") && !attributes[i].equals("diagnosis") &&
					!attributes[i].equals("dataset")) {
				predictors.add(attributes[i]);
			}
		}
		return predictors;
	}

	public static double distance(String a, String b) {
		String[] arrayA = a.split(",");
		String[] arrayB = b.split(",");
		double distance = 0;
		for (int i = 0; i < getPredictors().size(); i++) {
			String valueA = Utils_1NN.getAttribute(arrayA, getPredictors().get(i));
			String valueB = Utils_1NN.getAttribute(arrayB, getPredictors().get(i));

			double doubleA = Double.parseDouble(valueA);
			double doubleB = Double.parseDouble(valueB);

			distance += Math.pow(doubleA-doubleB, 2);
		}
		distance = Math.sqrt(distance);
		return distance;
	}

}
