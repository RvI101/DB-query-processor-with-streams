package dubstep.operators;

public class CrossProduct extends Operator {
    Operator left;
    Operator right;

    public CrossProduct(Operator left, Operator right) {
        this.left = left;
        this.right = right;
    }

    public CrossProduct() {
    }

    public Operator getLeft() {
        return left;
    }

    public void setLeft(Operator left) {
        this.left = left;
    }

    public Operator getRight() {
        return right;
    }

    public void setRight(Operator right) {
        this.right = right;
    }
}
