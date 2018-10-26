package com.hazelcast.enterprise;

/**
 * Contains sample licenses for testing purposes.
 */
public final class SampleLicense {

    private SampleLicense() {
    }

    public static final String EXPIRED_ENTERPRISE_LICENSE
            = "HazelcastEnterprise-3.6#2Nodes#OYFEaVrkfTbu3B6Uy7IwKc281e640D04212Dg40321014C000qP1X0Gq004M";
    public static final String TWO_NODES_ENTERPRISE_LICENSE
            = "HazelcastEnterprise#2Nodes#7NTklfabBjcuiYm51O3ErU110L000e2029v0gZ1Lg099p1P0LW1s041P920G";
    public static final String ENTERPRISE_LICENSE_WITHOUT_HUMAN_READABLE_PART
            = "IFEJwam5OUVBk6fyuT0rYi209DC92Dn91419Pxss10s01o0000o0s2x2000Q";
    public static final String V5_UNLIMITED_LICENSE
            = "UNLIMITED_LICENSE#99Nodes#dJfAlwHyCkXMO8gUj6NSQB5DmuT29W0KPGin1qYEbZ11000009010199011010909008100091191009000190";
    // this is a V4 unlimited license
    public static final String UNLIMITED_LICENSE
            = "UNLIMITED_LICENSE#99Nodes#VuE0OIH7TbfKwAUNmSj1JlyFkr6a53911000199920009119011112151009";
    public static final String V5_SECURITY_ONLY_LICENSE
            = "SECURITY_ONLY#99Nodes#JMnyKq1lCi6Q5DOXW0ufTb8wPGd2kmUBNYSgE9HZAj54090900000990090800000000091090000011009009";
    public static final String SECURITY_ONLY_LICENSE
            = "SECURITY_ONLY#10Nodes#f07AjTFSwlbNaJHVym5kUKOIuEr616000091913001190000090010090200";
    public static final String LICENSE_WITH_DIFFERENT_VERSION
            = "HazelcastEnterprise-3.6#10Nodes#i1FrKmSIRONz3T0AlkBYyU6790p19092d22X99p090Msg1G4nt13XM99699v";
    public static final String LICENSE_WITH_SMALLER_VERSION
            = "HazelcastEnterprise#2Nodes#2Clients#HDMemory:1024GB#OFN7iUaVTmjIB6SRArKc5bw319000240o011003021042q5Q0n1p0QLq30Wo";
    public static final String TWO_GB_V2_HD_LICENSE
            = "HazelcastEnterprise#10Nodes#HDMemory:2GB#Bjc6rNm3yw5lHkVIbT0EAu71550t0h9901M1q00s9t0n029o2Y101000t099";
    public static final String ENTERPRISE_HD_LICENSE
            = "HazelcastEnterpriseHD#2Nodes#maOHFiwR5YEcy1T6K7bJ0u290q21h9d19g00sX99C39399eG99Z9v0x9t9x0";
    public static final String V4_LICENSE_WITH_HD_MEMORY_DISABLED
            = "HD_MEMORY_DISABLED#10Nodes#brKfaS6VjJNlE0IUwmT15uHyFA7kO2111100200210101195901009091101";
    public static final String V4_LICENSE_WITH_SECURITY_DISABLED
            = "SECURITY_DISABLED#10Nodes#5w0yHlJK7Uua6NbmfATFj1SrkVEOI1811011110900101101999500220000";
    public static final String V4_LICENSE_WITH_HOT_RESTART_DISABLED
            = "HOT_RESTART_DISABLED#10Nodes#bTKAOE1wjaFf6VyNJlHI5umr07kUS1010011050210109990911102100100";
}
