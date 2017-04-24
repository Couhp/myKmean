package hadoop.myKmean;

public class Point {
	double x;
	double y;

	public Point(double x, double y) {
		this.x = x;
		this.y = y;
	}

	public Point(String str) {
		String[] p = str.split(" ");
		this.x = Double.parseDouble(p[0]);
		this.y = Double.parseDouble(p[1]);
	}
	
	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}
	
	public double distanceTo(Point p) {
		double dis = Math.sqrt(Math.pow((this.x - p.getX()), 2) + Math.pow((this.y - p.getY()), 2)); 
		return dis;
	}
	
	public String toString() {
		String str = Double.toString(x) + " " + Double.toString(y);
		return str;
	}

}
