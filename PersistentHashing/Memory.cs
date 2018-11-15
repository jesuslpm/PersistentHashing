/*
Copyright 2018 Jesús López Méndez

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


namespace PersistentHashing
{

    public class Memory
    {

        public static unsafe void ZeroMemory(byte* destination, long size)
        {
            if (size == 0) return;
            byte* end = destination + size;
            while (end >= destination + 8)
            {
                *(long*)destination = 0L;
                destination += 8;
            }
            if (end >= destination + 4)
            {
                *(int*)destination = 0;
                destination += 4;
            }
            if (end >= destination + 2)
            {
                *(short*)destination = 0;
                destination += 2;
            }
            if (end >= destination + 1)
            {
                *destination = 0;
            }
        }
    }
}
