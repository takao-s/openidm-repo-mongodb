/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Copyright © 2011 ForgeRock AS.
 * Portions Copyrighted 2013 Takao Sekiguchi.
 * All rights reserved.
 *
 * The contents of this file are subject to the terms
 * of the Common Development and Distribution License
 * (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at
 * http://forgerock.org/license/CDDLv1.0.html
 * See the License for the specific language governing
 * permission and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL
 * Header Notice in each file and include the License file
 * at http://forgerock.org/license/CDDLv1.0.html
 * If applicable, add the following below the CDDL Header,
 * with the fields enclosed by brackets [] replaced by
 * your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 */
package org.forgerock.openidm.repo.mongodb.impl;

import org.forgerock.openidm.objset.ConflictException;
import org.testng.Assert;
import org.testng.annotations.Test;

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
            Assert.fail("parseVersion failed");
        }
        
        Assert.fail("Not yet implemented");
    }

}
