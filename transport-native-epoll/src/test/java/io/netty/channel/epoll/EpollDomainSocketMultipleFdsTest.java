/*
 * Copyright 2026 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.channel.epoll;

import io.netty.channel.unix.DomainSocketAddress;
import io.netty.channel.unix.FileDescriptor;
import io.netty.channel.unix.tests.UnixTestUtils;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.PlatformDependent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class EpollDomainSocketMultipleFdsTest {

    // Connects to the unix domain socket given as first argument and sends the write ends of N pipes (second
    // argument) within a single SCM_RIGHTS message, which is more than recvFd() can accept. Afterwards it reports if
    // the receiver closed all of these fds again, which is visible as EOF on the read ends of the pipes.
    private static final String SENDER_SOURCE =
            "#include <poll.h>\n" +
            "#include <stdio.h>\n" +
            "#include <stdlib.h>\n" +
            "#include <string.h>\n" +
            "#include <unistd.h>\n" +
            "#include <sys/socket.h>\n" +
            "#include <sys/un.h>\n" +
            "\n" +
            "int main(int argc, char** argv) {\n" +
            "    struct sockaddr_un addr;\n" +
            "    struct msghdr msg;\n" +
            "    struct iovec iov;\n" +
            "    struct cmsghdr* cmsg;\n" +
            "    struct pollfd pfd;\n" +
            "    char data = 'x';\n" +
            "    char buf;\n" +
            "    char* control;\n" +
            "    int* readFds;\n" +
            "    int* writeFds;\n" +
            "    int pipeFds[2];\n" +
            "    int nfds;\n" +
            "    int leaked = 0;\n" +
            "    int i;\n" +
            "    int s;\n" +
            "\n" +
            "    if (argc != 3) {\n" +
            "        fprintf(stdout, \"usage: %s <path> <nfds>\\n\", argv[0]);\n" +
            "        return 1;\n" +
            "    }\n" +
            "    nfds = atoi(argv[2]);\n" +
            "    s = socket(AF_UNIX, SOCK_STREAM, 0);\n" +
            "    memset(&addr, 0, sizeof(addr));\n" +
            "    addr.sun_family = AF_UNIX;\n" +
            "    strncpy(addr.sun_path, argv[1], sizeof(addr.sun_path) - 1);\n" +
            "    if (s < 0 || connect(s, (struct sockaddr*) &addr, sizeof(addr)) != 0) {\n" +
            "        perror(\"connect\");\n" +
            "        return 1;\n" +
            "    }\n" +
            "    readFds = (int*) calloc(nfds, sizeof(int));\n" +
            "    writeFds = (int*) calloc(nfds, sizeof(int));\n" +
            "    for (i = 0; i < nfds; i++) {\n" +
            "        if (pipe(pipeFds) != 0) {\n" +
            "            perror(\"pipe\");\n" +
            "            return 1;\n" +
            "        }\n" +
            "        readFds[i] = pipeFds[0];\n" +
            "        writeFds[i] = pipeFds[1];\n" +
            "    }\n" +
            "    control = (char*) calloc(1, CMSG_SPACE(nfds * sizeof(int)));\n" +
            "    memset(&msg, 0, sizeof(msg));\n" +
            "    iov.iov_base = &data;\n" +
            "    iov.iov_len = 1;\n" +
            "    msg.msg_iov = &iov;\n" +
            "    msg.msg_iovlen = 1;\n" +
            "    msg.msg_control = control;\n" +
            "    msg.msg_controllen = CMSG_SPACE(nfds * sizeof(int));\n" +
            "    cmsg = CMSG_FIRSTHDR(&msg);\n" +
            "    cmsg->cmsg_level = SOL_SOCKET;\n" +
            "    cmsg->cmsg_type = SCM_RIGHTS;\n" +
            "    cmsg->cmsg_len = CMSG_LEN(nfds * sizeof(int));\n" +
            "    memcpy(CMSG_DATA(cmsg), writeFds, nfds * sizeof(int));\n" +
            "    if (sendmsg(s, &msg, 0) != 1) {\n" +
            "        perror(\"sendmsg\");\n" +
            "        return 1;\n" +
            "    }\n" +
            "    for (i = 0; i < nfds; i++) {\n" +
            "        close(writeFds[i]);\n" +
            "    }\n" +
            "    fprintf(stdout, \"sent\\n\");\n" +
            "    fflush(stdout);\n" +
            "    for (i = 0; i < nfds && !leaked; i++) {\n" +
            "        pfd.fd = readFds[i];\n" +
            "        pfd.events = POLLIN;\n" +
            "        pfd.revents = 0;\n" +
            "        if (poll(&pfd, 1, 10000) <= 0 || read(readFds[i], &buf, 1) != 0) {\n" +
            "            leaked = 1;\n" +
            "        }\n" +
            "    }\n" +
            "    fprintf(stdout, leaked ? \"leaked\\n\" : \"closed\\n\");\n" +
            "    fflush(stdout);\n" +
            "    while (read(STDIN_FILENO, &buf, 1) > 0) {\n" +
            "    }\n" +
            "    close(s);\n" +
            "    return 0;\n" +
            "}\n";

    private static File senderSource;
    private static File sender;
    private static String compileError;

    @BeforeAll
    public static void setup() throws Exception {
        Epoll.ensureAvailability();

        String compiler = findCompiler();
        if (compiler == null) {
            return;
        }
        senderSource = PlatformDependent.createTempFile("netty-send-fds", ".c", null);
        sender = new File(senderSource.getAbsolutePath() + ".bin");
        OutputStream out = new FileOutputStream(senderSource);
        try {
            out.write(SENDER_SOURCE.getBytes(CharsetUtil.US_ASCII));
        } finally {
            out.close();
        }
        Process process = new ProcessBuilder(compiler, "-o", sender.getAbsolutePath(),
                senderSource.getAbsolutePath()).redirectErrorStream(true).start();
        process.getOutputStream().close();
        String output = readFully(process.getInputStream());
        if (process.waitFor() != 0 || !sender.canExecute()) {
            compileError = "failed to compile fd sender: " + output;
        }
    }

    @AfterAll
    public static void cleanup() {
        if (senderSource != null) {
            senderSource.delete();
        }
        if (sender != null) {
            sender.delete();
        }
    }

    @Test
    @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
    public void testRecvFdClosesFdsIfPeerSendsMultipleFds() throws Exception {
        // Both fds fit into the control buffer used by recvFd() so no truncation happens.
        testRecvFdClosesAllFds(2);
    }

    @Test
    @Timeout(value = 30000, unit = TimeUnit.MILLISECONDS)
    public void testRecvFdClosesFdsIfAncillaryDataIsTruncated() throws Exception {
        // Not all fds fit into the control buffer used by recvFd() so MSG_CTRUNC is reported.
        testRecvFdClosesAllFds(16);
    }

    private static void testRecvFdClosesAllFds(int numFds) throws Exception {
        assumeTrue(sender != null, "a C compiler is needed to send more than one fd within a single message");
        assertTrue(compileError == null, compileError);

        DomainSocketAddress address = UnixTestUtils.newDomainSocketAddress();
        LinuxSocket server = LinuxSocket.newSocketDomain();
        LinuxSocket accepted = null;
        Process process = null;
        int received = 0;
        try {
            server.bind(address);
            server.listen(1);

            process = new ProcessBuilder(sender.getAbsolutePath(), address.path(), String.valueOf(numFds))
                    .redirectErrorStream(true).start();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), CharsetUtil.US_ASCII));
            assertEquals("sent", reader.readLine());

            accepted = new LinuxSocket(accept(server));
            IOException error = null;
            try {
                received = accepted.recvFd();
            } catch (IOException e) {
                error = e;
            }
            String verdict = reader.readLine();

            assertNotNull(error, "recvFd() must fail if more than one fd is received, but returned " + received);
            assertEquals("closed", verdict, "recvFd() leaked a received fd");
        } finally {
            if (received > 0) {
                new FileDescriptor(received).close();
            }
            if (accepted != null) {
                accepted.close();
            }
            server.close();
            if (process != null) {
                process.getOutputStream().close();
                process.waitFor();
            }
            new File(address.path()).delete();
        }
    }

    private static int accept(LinuxSocket server) throws Exception {
        byte[] addr = new byte[64];
        for (;;) {
            int fd = server.accept(addr);
            if (fd >= 0) {
                return fd;
            }
            Thread.sleep(10);
        }
    }

    private static String findCompiler() {
        for (String compiler : new String[] { "gcc", "cc" }) {
            try {
                Process process = new ProcessBuilder(compiler, "--version").redirectErrorStream(true).start();
                process.getOutputStream().close();
                readFully(process.getInputStream());
                if (process.waitFor() == 0) {
                    return compiler;
                }
            } catch (Exception ignore) {
                // try the next one
            }
        }
        return null;
    }

    private static String readFully(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = in.read(buffer)) != -1) {
            out.write(buffer, 0, read);
        }
        return new String(out.toByteArray(), CharsetUtil.US_ASCII);
    }
}
