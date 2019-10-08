// Code adapted from Geobroker project : https://github.com/MoeweX/geobroker

package com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.NotNull;
import org.locationtech.spatial4j.io.ShapeWriter;
import org.locationtech.spatial4j.shape.*;

import java.text.ParseException;
import java.util.List;
import java.util.Objects;

import static com.chaitas.distributed.geobroker.Messages.ExternalMessages.Spatial.SpatialContext.GEO;


public class Geofence{
    @JsonIgnore
    private final Shape shape;

    // TODO increase size a little bit so that we do not miss any due to rounding issues
    // we need it most times anyways, so let's buffer it //
    @JsonIgnore
    public final Rectangle boundingBox;

    @JsonIgnore
    private Geofence(Shape shape) {
        this.shape = shape;
        this.boundingBox = shape.getBoundingBox();
    }

    public Geofence(@NotNull @JsonProperty("wkt") String wkt) throws ParseException {
        WKTReader reader = (WKTReader) GEO.getFormats().getWktReader();
        this.shape = reader.parse(wkt);
        this.boundingBox = this.shape.getBoundingBox();
    }

    /**
     * Creates a new geofence based on the supplied locations
     *
     * @param surroundingLocations - the locations that surround the geofence
     * @return a new geofence
     * @throws RuntimeShapeException if invalid param
     */
    public static Geofence polygon(List<Location> surroundingLocations) throws Exception {
        if (surroundingLocations.size() < 3) {
            throw new Exception("A geofence needs at least 3 locations");
        }

        final ShapeFactory.PolygonBuilder polygonBuilder = GEO.getShapeFactory().polygon();
        for (Location location : surroundingLocations) {
            polygonBuilder.pointXY(location.getLon(), location.getLat());
        }
        // close polygon
        polygonBuilder.pointXY(surroundingLocations.get(0).getLon(), surroundingLocations.get(0).getLat());
        return new Geofence(polygonBuilder.build());
    }

    @JsonIgnore
    public static Geofence rectangle(Location southWest, Location northEast) {
        Rectangle r = GEO.getShapeFactory().rect(southWest.getPoint(), northEast.getPoint());
        return new Geofence(r);
    }
    @JsonIgnore
    public static Geofence circle(Location location, double radiusDegree) {
        Circle c = GEO.getShapeFactory().circle(location.getPoint(), radiusDegree);
        return new Geofence(c);
    }
    @JsonIgnore
    public static Geofence world() {
        Shape worldShape = GEO.getWorldBounds();
        return new Geofence(worldShape);
    }
    @JsonIgnore
    public boolean isRectangle() {
        return GEO.getShapeFactory().getGeometryFrom(shape).isRectangle();
    }

    /*****************************************************************
     * BoundingBox
     ****************************************************************/
    @JsonIgnore
    public Location getBoundingBoxNorthWest() {
        return new Location(boundingBox.getMaxY(), boundingBox.getMinX());
    }
    @JsonIgnore
    public Location getBoundingBoxNorthEast() {
        return new Location(boundingBox.getMaxY(), boundingBox.getMaxX());
    }
    @JsonIgnore
    public Location getBoundingBoxSouthEast() {
        return new Location(boundingBox.getMinY(), boundingBox.getMaxX());
    }
    @JsonIgnore
    public Location getBoundingBoxSouthWest() {
        return new Location(boundingBox.getMinY(), boundingBox.getMinX());
    }

    /**
     * See {@link Rectangle#getHeight()}, is the latitude distance in degree
     */
    @JsonIgnore
    public double getBoundingBoxLatDistanceInDegree() {
        return boundingBox.getHeight();
    }

    /**
     * See {@link Rectangle#getWidth()}, is the longitude distance in degree
     */
    @JsonIgnore
    public double getBoundingBoxLonDistanceInDegree() {
        return boundingBox.getWidth();
    }

    @JsonIgnore
    public boolean contains(Location location) {
        if (location.isUndefined()) {
            return false;
        }
        return shape.relate(location.getPoint()).equals(SpatialRelation.CONTAINS);
    }

    /**
     * For us, intersects is an "intersection" but also something more specific such as "contains" or within.
     */
    @JsonIgnore
    public boolean intersects(Geofence geofence) {
        SpatialRelation sr = shape.relate(geofence.shape);
        return sr.equals(SpatialRelation.INTERSECTS) || sr.equals(SpatialRelation.CONTAINS) ||
                sr.equals(SpatialRelation.WITHIN);
    }

    @JsonIgnore
    public boolean disjoint(Geofence geofence) {
        return shape.relate(geofence.shape).equals(SpatialRelation.DISJOINT);
    }

    /*****************************************************************
     * Getters and String
     ****************************************************************/
    @JsonIgnore
    public Shape getShapeObject() {
        return this.shape;
    }

    public String getWKT() {
        ShapeWriter writer = GEO.getFormats().getWktWriter();
        return writer.toString(shape);
    }

    @Override
    public String toString() {
        return getWKT();
    }

    /*****************************************************************
     * Generated methods
     ****************************************************************/

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Geofence geofence = (Geofence) o;
        return Objects.equals(shape, geofence.shape);
    }

    @Override
    public int hashCode() {

        return Objects.hash(shape);
    }

    @JsonIgnore
    public static void main(String[] args) {
        Location paris = new Location(48.86, 2.35);
        Location berlin = new Location(52.52, 13.40);
        Geofence parisArea = Geofence.circle(paris, 3.0);
        Geofence berlinArea = Geofence.circle(berlin, 3.0);

        System.out.println("Paris area = " + parisArea);
        System.out.println("Berlin area = " + berlinArea);
        System.out.println("The areas intersect: " + berlinArea.intersects(parisArea));

        Location justIn = new Location(45.87, 2.3);
        System.out.println(parisArea.contains(justIn));

        System.out.println("Contains check Benchmark");
        Geofence world = Geofence.world();
        long amount = 10000000;

        long time = System.nanoTime();
        for (int i = 0; i < amount; i++) {
            berlinArea.contains(berlin);
        }
        System.out.println("{} berlin in circle checks per ms : " + amount * 1000 * 1000 / (System.nanoTime() - time));

        time = System.nanoTime();
        for (int i = 0; i < amount; i++) {
            berlinArea.contains(paris);
        }
        System.out.println("{} berlin out circle checks per ms : " + amount * 1000 * 1000 / (System.nanoTime() - time));

        time = System.nanoTime();
        for (int i = 0; i < amount; i++) {
            world.contains(berlin);
        }
        System.out.println("{} world checks per ms : " + amount * 1000 * 1000 / (System.nanoTime() - time));
    }
}
