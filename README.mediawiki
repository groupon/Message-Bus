=message-bus=

MessageBus is a distributed messaging platform built on top of hornetq, and supports java and ruby client. This project includes a patch to HornetQ, and the java and ruby client that works with it.

==Building the server==

===Check out and patch server===
 $ ./checkout-hornetq.sh

This step first checks out the Jboss HornetQ project, then applies a patch on top of it. It can take a few minutes depending on your internet speed.

===Build the server===
If in Unix/Linux:

 $ cd hornetq
 $ ./build.sh bin-distro

If in OSX:

 $ cd hornetq
 $ ./mac-build.sh bin-distro

The server build is located in hornetq/build/hornetq-2.2.22.SNAPSHOT