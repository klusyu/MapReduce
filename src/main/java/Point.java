public class Point {
    private float[] x;
    public Point(float... x) {
        this.x = x;
    }
    public float distance(Point p) throws Exception {
        if (p.x.length != x.length) {
            throw new Exception("Different dimensions");
        }
        return manhattanDistance(p);
    }

    public float euclideanDistance(Point p) {

        float sum = 0.0f;
        for (int i = 0; i < x.length; ++i) {
            sum += Math.pow(x[i] - p.x[i], 2);
        }
        return (float) Math.sqrt(sum);
    }

    public float manhattanDistance(Point p) {
        float sum = 0.0f;
        for (int i = 0; i < x.length; ++i) {
            sum += Math.abs(x[i] - p.x[i]);
        }
        return sum;
    }

    public boolean compare(Point p) {
        if (x.length != p.x.length) {
            return false;
        }
        for(int i = 0; i < x.length; ++i) {
            if (x[i] != p.x[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        String s = Float.toString(x[0]);
        for(int i = 1; i < x.length; ++i) {
            s += "," + x[i];
        }
        return s;
    }

    public String toString(String delimeter) {
        String s = Float.toString(x[0]);
        for(int i = 1; i < x.length; ++i) {
            s += delimeter + x[i];
        }
        return s;
    }
}
