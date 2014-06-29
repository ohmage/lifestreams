package org.ohmage.lifestreams.oauth;

/**
 * Created by changun on 6/27/14.
 */
public class Scope {
    private String provider;
    private String scopeName;

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getScopeName() {
        return scopeName;
    }

    public void setScopeName(String scope) {
        this.scopeName = scope;
    }

    @Override
    public String toString() {
        return "Scope{" +
                "provider='" + provider + '\'' +
                ", scopeName='" + scopeName + '\'' +
                '}';
    }

    public Scope(String provider, String scopeName) {
        this.provider = provider;
        this.scopeName = scopeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Scope)) return false;

        Scope scope = (Scope) o;

        if (!provider.equals(scope.provider)) return false;
        if (!scopeName.equals(scope.scopeName)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = provider.hashCode();
        result = 31 * result + scopeName.hashCode();
        return result;
    }
}
