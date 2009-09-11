Permanence
==========

Permanence lets you record scheduled shows from an audio source ([JACK][jack],
an Internet radio stream, etc.) and save them to disk or a server. Some of
Permanence's features:

- Flexible storage: recorded files are passed to storage drivers which can do
  anything they want with the files: copy them to a new location on disk,
  upload them to a server, etc.
- Extensible: you can provide new source and storage drivers to handle
  technologies that Permanence doesn't cover out of the box
- Open: you can install scripts and Python functions as Permanence "hooks" that
  will get called when specific events happen, such as when Permanence finishes
  recording a show

Permanence has been used internally by [KRLX radio][krlx] to record copies of
their productions since May 2009. If you think Permanence could be useful for
your needs, contact us at [computing@krlx.org](mailto:computing@krlx.org).

[jack]: http://www.jackaudio.org/
[krlx]: http://www.krlx.org/

License
-------

Permanence is made available under the terms of the MIT License.

Copyright © 2009 [Eric Naeseth][copyright_holder]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

[copyright_holder]: http://github.com/enaeseth/
