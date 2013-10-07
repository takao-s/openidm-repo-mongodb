package org.forgerock.openidm.repo.mongodb.impl;

import static org.junit.Assert.*;

import org.forgerock.openidm.objset.ConflictException;
import org.junit.Test;
import org.testng.Assert;

/**
 * DocumentUnit test
 * 
 * @author takao-s
 */
public class DocumentUtilTest {
    String message = "MongoDB repository expects revisions as int, " 
            + "unable to parse passed revision: ";
    @Test
    public void testParseVersion() {
        String revision = null;
        try {
            DocumentUtil.parseVersion(revision);
        } catch (ConflictException e) {
            Assert.assertEquals(e.getMessage(), message + revision);
        }
        
        revision = "a";
        try {
            DocumentUtil.parseVersion(revision);
        } catch (ConflictException e) {
            Assert.assertEquals(e.getMessage(), message + revision);
        }
        
        revision = "1";
        try {
            DocumentUtil.parseVersion(revision);
            return;
        } catch (ConflictException e) {
            fail("parseVersion failed");
        }
        
        fail("Not yet implemented");
    }

}
